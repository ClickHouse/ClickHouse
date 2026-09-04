# Redis and Memcached Key-Value Project Analysis

This note starts the analysis phase for ClickHouse issue [#33581](https://github.com/ClickHouse/ClickHouse/issues/33581), `Key-value Data Marts`.

## Motivation

The issue asks for `clickhouse-server` to serve a large amount of short queries on prepared datasets. The target use cases are user personalization, antifraud, real-time bidding, DDoS prevention, e-commerce, and games.

ClickHouse already prepares and stores analytical datasets efficiently, but many serving systems expect a very small lookup surface: one key, a few returned values, and protocol latency closer to a cache than to an SQL client. A Redis or Memcached-compatible endpoint would let existing applications read selected ClickHouse-backed datasets without running SQL and without deploying an extra serving cache for every prepared data mart.

## Protocol Server Registration

Protocol servers are registered from `programs/server/Server.cpp`.

The main entry point is `Server::createServers`. It iterates over configured `listen_host` values and conditionally creates servers for `http_port`, `https_port`, `tcp_port`, `tcp_with_proxy_port`, `tcp_port_secure`, `tcp_ssh_port`, `mysql_port`, `postgresql_port`, `grpc_port`, `prometheus.port`, and `arrowflight_port` when the corresponding `ServerType` is enabled.

Each endpoint is constructed through `Server::createServer`, which binds the socket, registers the configured port through `Context::registerServerPort`, and stores a `ProtocolServerAdapter`. TCP-like protocol handlers are exposed as `TCPServerConnectionFactory` implementations and wrapped in `TCPServer`.

There is also a custom composable protocol path under `Server::buildProtocolStackFromConfig`. It reads `protocols.<name>` entries, builds a `TCPProtocolStackFactory`, and supports layer types such as `tcp`, `tls`, `proxy1`, `mysql`, `postgres`, `http`, `prometheus`, and `interserver`.

## Reference Protocol Handlers

Useful references for a future Redis or Memcached handler:

- `src/Server/TCPHandler.h` and `src/Server/TCPHandler.cpp`: native ClickHouse TCP protocol, long-lived sessions, query execution, progress, settings, compression, and connection lifetime handling.
- `src/Server/TCPHandlerFactory.h`: minimal factory pattern for creating a `TCPHandler` from a `Poco::Net::StreamSocket`.
- `src/Server/MySQLHandler.h`, `src/Server/MySQLHandler.cpp`, and `src/Server/MySQLHandlerFactory.h`: compatibility protocol over `TCPServerConnection`, authentication handshake, packet framing, query replacement, and session setup with `ClientInfo::Interface::MYSQL`.
- `src/Server/PostgreSQLHandler.h`, `src/Server/PostgreSQLHandler.cpp`, and `src/Server/PostgreSQLHandlerFactory.h`: compatibility protocol loop with startup, authentication, message dispatch, and session setup with `ClientInfo::Interface::POSTGRESQL`.
- `src/Server/KeeperTCPHandler.h` and `src/Server/KeeperTCPHandlerFactory.h`: a compact non-SQL TCP handler and factory pair.
- `src/Server/GRPCServer.h` and `src/Server/GRPCServer.cpp`: non-`TCPServerConnection` registration through `IGRPCServer`, useful as a contrast rather than a direct model.
- `src/Server/TCPProtocolStackFactory.h`: support for custom protocol stacks configured under `protocols`.

## `IKeyValueEntity` Interface

`IKeyValueEntity` is defined in `src/Interpreters/IKeyValueEntity.h`. It is an interface for entities with direct key-value lookup semantics.

Main methods:

- `getPrimaryKey`: returns the key column names accepted by direct key-value lookup. The key may contain multiple columns.
- `getByKeys`: accepts key columns, requested result column names, and output containers for missing-key state and `ALL` join offsets. It returns a `Chunk` containing rows for the requested keys. If `out_offsets` stays empty, the result row count matches the input key count and missing keys are represented by default values plus the null map. If `out_offsets` is filled, the result may contain a different number of rows for `ALL` join semantics.
- `getSampleBlock`: returns the header for `getByKeys` results, optionally restricted to requested columns.
- `key_value_result_names`: protected storage for result column names, currently available to implementations but not a public contract.

## Current `IKeyValueEntity` Implementations

The current repository shows direct inheritance from `IKeyValueEntity` in these classes:

- `IDictionary` in `src/Dictionaries/IDictionary.h`: all dictionaries inherit `IKeyValueEntity`; `getByKeys` is implemented in terms of dictionary `hasKeys` and `getColumns`.
- `StorageEmbeddedRocksDB` in `src/Storages/RocksDB/StorageEmbeddedRocksDB.h` and `src/Storages/RocksDB/StorageEmbeddedRocksDB.cpp`: local `RocksDB`-backed key-value storage with single or multi-column primary keys.
- `StorageKeeperMap` in `src/Storages/StorageKeeperMap.h` and `src/Storages/StorageKeeperMap.cpp`: `KeeperMap` storage backed by ZooKeeper or ClickHouse Keeper. It supports one key column and uses batched `tryGet`.
- `StorageRedis` in `src/Storages/StorageRedis.h` and `src/Storages/StorageRedis.cpp`: ClickHouse table engine that reads from an external Redis server. It supports one key column and uses serialized keys plus Redis `MGET`. It is useful as an example of `IKeyValueEntity` usage and Redis value conversion, but it should not be treated as the main recommended backend for a new Redis-compatible server endpoint.
- `DirectJoinMergeTreeEntity` in `src/Interpreters/DirectJoinMergeTreeEntity.h` and `src/Interpreters/DirectJoinMergeTreeEntity.cpp`: an adapter used by direct joins to execute lookup plans against `MergeTree` data.

`StorageJoin` and `StorageSet` are related but different. `StorageJoin` has its own direct join path through `StorageJoin` and `HashJoin`; it is not currently an `IKeyValueEntity`. `StorageSet` is optimized for `IN` sets, not value retrieval.

## Already Covered Architecture

Parts of issue `#33581` are already covered:

- A protocol registration framework exists in `clickhouse-server`: adding a new port and handler follows the same shape as `mysql_port` and `postgresql_port`.
- Compatibility protocol handlers already exist for MySQL and PostgreSQL, proving that `clickhouse-server` can expose non-native wire protocols.
- A direct key-value abstraction exists as `IKeyValueEntity`; it is no longer necessary to add a first version of `get` directly to `IStorage`.
- Several entities already expose direct lookup semantics: dictionaries, `EmbeddedRocksDB`, `KeeperMap`, `StorageRedis`, and the `MergeTree` direct-join adapter.
- Direct joins already use `IKeyValueEntity` through planner paths in `PlannerJoins`, `PlannerJoinsLogical`, `JoinedTables`, `TableJoin`, and `DirectJoin`.

## Missing Redis And Memcached Access

The missing parts are mostly serving-facing:

- No `RedisHandler`, `RedisHandlerFactory`, `MemcachedHandler`, or `MemcachedHandlerFactory` exists under `src/Server`.
- No config key such as `redis_port` or `memcached_port` exists for a Redis or Memcached-compatible endpoint.
- No `ServerType` entry exists for Redis or Memcached.
- No mapping exists from Redis logical database numbers to ClickHouse databases, tables, dictionaries, or named key-value entities.
- No endpoint-level authorization model exists for Redis `AUTH`, unauthenticated local access, user/profile mapping, quotas, or readonly guarantees.
- No RESP parser/writer exists for Redis server-side command handling.
- No Memcached text protocol parser/writer exists.
- No common helper exists to resolve a key-value target and call `IKeyValueEntity::getByKeys` from a protocol handler.
- No value encoding contract exists: a Redis bulk string must be produced from ClickHouse typed columns, but the project still needs to define whether values are strings, JSON, TSV fields, Native fragments, or a single selected column.

## Smallest Redis-Compatible MVP

The smallest Redis-compatible MVP should be read-only and RESP-based:

- `PING`: return `PONG`, or echo the optional message if supported.
- `QUIT`: return `OK` and close the connection.
- `SELECT`: accept a numeric database id and store it in connection state. Initially this can select a configured key-value target rather than a real Redis database.
- `GET`: parse one key, call `IKeyValueEntity::getByKeys`, and return a bulk string or null bulk string.
- `MGET`: parse multiple keys, call `IKeyValueEntity::getByKeys` once, and return an array of bulk strings or null bulk strings in input order.

The MVP should probably restrict targets to single-column keys and one value representation. The first demonstration targets should be `EmbeddedRocksDB` and dictionaries because they represent ClickHouse-managed key-value data or prepared lookup data. `StorageRedis` can still be useful as a code reference, but using an external Redis-backed table as the backend for a Redis-compatible ClickHouse endpoint would not demonstrate the intended architecture.

## Optional Extensions

Useful extensions after the MVP:

- `HGET`: map a Redis hash key plus field to either a ClickHouse key plus selected result column, or to a `Map`/`Object`-like value representation.
- `HMGET`: batched version of `HGET`, preserving Redis ordering and null semantics.
- `AUTH`: map Redis password authentication to ClickHouse users, interoperate with users configuration, and avoid logging secrets.
- Memcached text protocol: implement `get`, `gets`, `version`, `quit`, and possibly `stats` for clients that expect Memcached instead of Redis.

## Likely Future Files

Likely added files:

- `src/Server/RedisHandler.h`
- `src/Server/RedisHandler.cpp`
- `src/Server/RedisHandlerFactory.h`
- `src/Server/RedisHandlerFactory.cpp`
- `src/Server/RedisProtocol.h`
- `src/Server/RedisProtocol.cpp`
- `src/Server/MemcachedHandler.h`
- `src/Server/MemcachedHandler.cpp`
- `src/Server/MemcachedHandlerFactory.h`
- `src/Server/MemcachedHandlerFactory.cpp`
- `src/Server/MemcachedProtocol.h`
- `src/Server/MemcachedProtocol.cpp`
- Integration tests under `tests/integration/test_redis_protocol` and optionally `tests/integration/test_memcached_protocol`.
- Stateless smoke tests under `tests/queries/0_stateless` if a lightweight client can be used without external services.

Likely changed files:

- `programs/server/Server.cpp`: register Redis and Memcached endpoints. For an early prototype, most endpoint registration can be done here plus config files.
- `programs/server/config.xml` and `programs/server/config.yaml.example`: add optional endpoint configuration if ports are exposed in sample configs.
- `src/Server/ServerType.h` and `src/Server/ServerType.cpp`: only needed if Redis or Memcached become first-class endpoint types for start, stop, and reload handling.
- `src/Common/setThreadName.h`: add handler thread names.
- `src/Common/AsynchronousMetrics.cpp` and `ProfileEvents` declarations if per-interface metrics are added.
- `src/Interpreters/ClientInfo.h` if new `ClientInfo::Interface` values are needed.
- `src/Interpreters/Context.h`, `src/Interpreters/Context.cpp`, or a new helper under `src/Interpreters` if target resolution is centralized.

## Risks And Limitations

- Redis and Memcached clients expect very low latency, stable wire semantics, and precise null handling. Any SQL planning or heavy conversion in the request path can dominate latency.
- `IKeyValueEntity::getByKeys` returns typed columns, but Redis and Memcached return bytes. The encoding contract is the core product decision.
- Multi-column keys and multi-column values do not map cleanly to simple `GET`.
- `DirectJoinMergeTreeEntity` can perform lookups through a query plan, but it is not obviously suitable as the first high-throughput serving path because it may still execute more machinery than a dedicated key-value storage.
- `StorageRedis` is a client for external Redis, not the desired server-side Redis-compatible endpoint.
- Authentication, quotas, readonly guarantees, and audit logging need a ClickHouse-native design, not a direct copy of Redis behavior.
- Large `MGET` requests can create memory pressure and should respect limits similar to `max_query_size`, maximum argument count, and maximum response size.
- Redis protocol compatibility can grow quickly. Keeping the MVP to `PING`, `QUIT`, `SELECT`, `GET`, and `MGET` limits surface area while validating the architecture.
