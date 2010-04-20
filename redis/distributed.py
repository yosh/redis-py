import random
import re
from redis.exceptions import UnsafeMultiNodeCommandError
from redis.client import Connection, ConnectionPool, Redis
from redis.hash_ring import HashRing

# regex used to look for shard hints in a key name
KEY_TOKEN_RE = re.compile(ur'\{([^\}]+)\}')

class HashRingConnectionPool(ConnectionPool):
    def __init__(self, nodes, socket_timeout=None,
                 charset='utf-8', errors='strict'):
        """
        ``nodes`` specifies a list of nodes in the cluster. Each item is a
        dictionary with the following keys:
            host: hostname or IP address of the instance
            port: (defaults to 6379)
            db: (defaults to 0)
            password: (defaults to None)
            weight: (defaults to 1)
        """
        super(HashRingConnectionPool, self).__init__()
        clients = []
        weights = {}
        for node in nodes:
            try:
                weight = int(node.get('weight', 1))
            except ValueError:
                raise RedisError("'weight' must be an integer")
            # don't modify source dict
            params = node.copy()
            params.update({
                'socket_timeout': socket_timeout,
                'connection_pool': self,
                'charset': charset,
                'errors': errors
                })
            client = Redis(**params)
            clients.append(client)
            weights[client] = weight
        self.hash_ring = HashRing(clients, weights)

    def get_all_clients(self):
        return self.hash_ring.nodes

    def get_client_for_key(self, key):
        "Return the client that ``key`` should be found on"
        match = KEY_TOKEN_RE.search(key)
        if match:
            key = match.group(1)
        return self.hash_ring.get_node(key)

    def get_client_map(self, keys):
        """
        Given a list of ``keys``, return a dictionary mapping of clients
        to a list of the keys that can be found on that client
        """
        mapping = {}
        for key in keys:
            mapping.setdefault(self.get_client_for_key(key), []).append(key)
        return mapping

class DistributedRedis(Redis):
    """
    Provides functionality for distributing keys across many Redis
    instances. Uses consistent hashing to minimize changes when
    adding or removing nodes.

    ``nodes`` specifies a list of nodes in the cluster. Each item is a
    dictionary with the following keys:
        host: hostname or IP address of the instance
        port: (defaults to 6379)
        db: (defaults to 0)
        password: (defaults to None)
        weight: (defaults to 1)
    """
    # commands that get executed on all nodes
    MULTINODE_COMMANDS = set([
        'BGREWRITEAOF', 'BGSAVE', 'DBSIZE', 'DEL', 'FLUSHDB', 'FLUSHALL',
        'INFO', 'KEYS', 'LASTSAVE', 'PING', 'SAVE'
        ])

    # commands that are unsafe to use in a multi-node environment
    UNSAFE_COMMANDS = set([
        'BLPOP', 'BRPOP', 'LISTEN', 'MGET', 'MOVE', 'MSET', 'MSETNX',
        'RANDOMKEY', 'RENAME', 'RENAMENX', 'RPOPLPUSH', 'SDIFF',
        'SDIFFSTORE', 'SINTER', 'SINTERSTORE', 'SMOVE', 'SORT', 'SUNION',
        'SUNIONSTORE'
        ])

    def __init__(self, nodes=[], socket_timeout=None,
                 connection_pool=None, charset='utf-8', errors='strict'):
        if not nodes and not connection_pool:
            raise RedisError("DistributedRedis requires either the 'nodes' "
                "or 'connection_pool' argument.")
        self.encoding = charset
        self.errors = errors
        self.connection = None
        self.subscribed = False
        self.connection_pool = connection_pool and connection_pool or \
            HashRingConnectionPool(
                nodes,
                socket_timeout=socket_timeout,
                charset=charset,
                errors=errors
                )

    def execute_command(self, *args, **options):
        cmd = args[0]
        if cmd in self.MULTINODE_COMMANDS:
            # iter over all nodes and run cmd
            return [client.execute_command(*args, **options)
                    for client in self.connection_pool.get_all_clients()]
        else:
            # default behavior is to examine piece #2 for the key.
            # commands that don't follow this pattern should be
            # re-implemented
            key = self.encode(args[1])
            client = self.connection_pool.get_client_for_key(key)
            return client.execute_command(*args, **options)

    # These aren't atomic anymore, but can be implemented easily.
    # Should we keep them?
    # def mget(self, keys):
    #     client_to_keys = self.connection_pool.get_client_map(keys)
    #     results = {}
    #     for client, keylist in client_to_keys.iteritems():
    #         results.update(dict(zip(keylist, client.mget(keylist))))
    #     return [results[k] for k in keys]
    # 
    # def mset(self, mapping):
    #     client_to_mapping = {}
    #     pool = self.connection_pool
    #     for k, v in mapping.iteritems():
    #         k = self.encode(k)
    #         client_to_mapping.setdefault(pool.get_client_for_key(k), {})[k] = v
    #     results = []
    #     for client, client_mapping in client_to_mapping.iteritems():
    #         results.append(client.mset(client_mapping))
    #     return all(results)

# Commands deemed unsafe should raise an UnsafeMultiNodeCommandError
def unsafe_multinode_command(self, *args, **kwargs):
    raise UnsafeMultiNodeCommandError

for cmd in DistributedRedis.UNSAFE_COMMANDS:
     setattr(DistributedRedis, cmd.lower(), unsafe_multinode_command)
