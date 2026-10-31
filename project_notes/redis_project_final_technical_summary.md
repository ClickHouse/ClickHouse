# Redis-Compatible Endpoint Final Technical Summary

## 1. Project goal

This thesis project extends ClickHouse with a Redis-compatible read-only endpoint for high-throughput key-value point lookups. The endpoint is intended for prepared ClickHouse key-value data marts, not for general-purpose Redis compatibility.

The main research question is whether a specialized Redis-compatible read-only endpoint can reduce the overhead of point lookups by bypassing the normal SQL/HTTP request path and using ClickHouse's existing direct key-value abstraction.

Careful thesis wording:

> Redis-compatible endpoint removes much of SQL/HTTP overhead for supported read-only point lookups over prepared ClickHouse key-value tables.

The project does not claim that ClickHouse is faster than Redis. Redis is used only as an external reference baseline in benchmarks.

## 2. ClickHouse internals relevant to this project

### Protocol server model

ClickHouse server listeners are registered in `programs/server/Server.cpp`. The implementation follows the existing TCP-style compatibility protocol model used by other server endpoints:

- `Server::createServers` creates protocol listeners when the relevant `ServerType` is enabled.
- `Server::createServer` binds the socket and stores a `ProtocolServerAdapter`.
- TCP-like handlers use `TCPServerConnectionFactory` and `TCPServer`.
- `RedisHandlerFactory` creates one `RedisHandler` per accepted TCP connection.

`ServerType` was extended with `REDIS`, and `ServerType::shouldStop` maps `redis_port` to `ServerType::Type::REDIS`. This makes `redis_port` part of the normal listener start/stop classification.

### Server registration

`programs/server/Server.cpp` includes `Server/RedisHandlerFactory.h`. In `Server::createServers`, when `ServerType::Type::REDIS` should start, it reads `redis_port`, binds the socket, and creates:

```text
TCPServer(new RedisHandlerFactory(*this), ...)
```

The listener description is:

```text
Redis compatibility protocol: <address>
```

### Configuration files

The config examples are:

- `programs/server/config.xml`
- `programs/server/config.yaml.example`

They now document the feature as a Redis-compatible read-only endpoint for prepared key-value tables, supporting `PING`, `QUIT`, `SELECT`, `GET`, and `MGET`.

### `DatabaseCatalog`

`RedisHandler` resolves the selected table through:

```text
DatabaseCatalog::instance().getTable(StorageID(database, table), context)
```

The selected `database`, `table`, and `default_column` come from the Redis DB mapping selected by `SELECT <db>`.

### `IKeyValueEntity`

`IKeyValueEntity` is defined in `src/Interpreters/IKeyValueEntity.h`. It is the ClickHouse interface for direct key-value lookup semantics.

Important methods:

- `IKeyValueEntity::getPrimaryKey`: returns key column names accepted by direct lookup.
- `IKeyValueEntity::getByKeys`: accepts key columns and requested result columns, fills a found/null map and offsets, and returns a `Chunk`.
- `IKeyValueEntity::getSampleBlock`: returns a result header for direct lookups.

The interface is already used by direct join infrastructure. The project reuses it rather than introducing a new storage interface.

### Relevant `IKeyValueEntity` implementations

Implementations found in the repository include:

- `StorageEmbeddedRocksDB`: local RocksDB-backed key-value storage. This is the main tested backend in the project.
- `IDictionary`: dictionaries expose key-value lookup through `hasKeys` and `getColumns`.
- `StorageKeeperMap`: Keeper-backed map storage with one key column.
- `StorageRedis`: ClickHouse table engine that reads from an external Redis server; useful as an example, not the target backend for this endpoint.
- `DirectJoinMergeTreeEntity`: direct join adapter for `MergeTree`.

The integration tests and benchmarks focus on `EmbeddedRocksDB`.

### SQL/HTTP path versus Redis-compatible path

SQL/HTTP path:

```text
Client -> HTTP -> SQL parser -> analyzer/planner -> query pipeline -> storage -> result
```

Redis-compatible path:

```text
Client -> TCP :9006 / RESP -> RedisProtocol -> RedisHandler -> DatabaseCatalog -> IKeyValueEntity::getByKeys -> RESP response
```

The Redis-compatible path does not go through the SQL parser, analyzer/planner, or query pipeline for supported `GET`/`MGET` requests.

## 3. Final implemented scope

Implemented commands:

- `PING`
- `QUIT`
- `SELECT`
- `GET`
- `MGET`

Endpoint properties:

- Read-only.
- RESP/TCP endpoint exposed through `redis_port`.
- `SELECT <db>` selects a configured ClickHouse target mapping for the current TCP connection.
- `GET key` returns a Redis bulk string for an existing key or Redis null bulk string for a missing key.
- `MGET key1 key2 ...` returns a Redis array containing bulk strings or null bulk strings.
- `MGET` preserves input key order.
- Missing keys are detected through the found map returned by `IKeyValueEntity::getByKeys`, not by inspecting default values.

Supported data scope:

- `IKeyValueEntity`-compatible tables.
- Single-column primary key only.
- `String` key to `String` value.
- `UInt64` key to `String` value.
- One configured `String` `default_column` value.

Not supported:

- `SET`
- `DEL`
- `TTL`
- `AUTH`
- `HGET`/`HMGET`
- Pub/Sub
- Lua
- Redis cluster
- arbitrary ClickHouse tables
- composite keys
- non-`String` values
- full Redis compatibility

## 4. Configuration model

The endpoint is enabled with:

```xml
<redis_port>9006</redis_port>
```

Redis logical DBs are mapped to ClickHouse targets:

```xml
<redis>
    <db>
        <_0>
            <database>default</database>
            <table>kv_baseline</table>
            <default_column>value</default_column>
        </_0>
    </db>
</redis>
```

Equivalent logical keys:

- `redis.db._N.database`
- `redis.db._N.table`
- `redis.db._N.default_column`

`SELECT <db>` maps to `redis.db._<db>`. If any of `database`, `table`, or `default_column` is absent or empty, the DB is treated as unconfigured.

The config comments in `programs/server/config.xml` and `programs/server/config.yaml.example` were updated to match the current implementation. They explicitly say that this is a read-only Redis-compatible endpoint, not a full Redis implementation or Redis replacement.

## 5. Implementation map

### `src/Server/RedisProtocol.h`

Defines the low-level RESP command and writer API:

- `RedisProtocol::Command` with `name` and `arguments`.
- `RedisProtocol::readCommand`
- `RedisProtocol::writeSimpleString`
- `RedisProtocol::writeError`
- `RedisProtocol::writeBulkString`
- `RedisProtocol::writeNullBulkString`
- `RedisProtocol::writeArrayHeader`

### `src/Server/RedisProtocol.cpp`

Implements minimal RESP parsing and response writing.

Parsing behavior:

- Requires a RESP array as the command envelope.
- Requires non-empty command arrays.
- Reads bulk string command name and arguments.
- Uppercases command name with ASCII uppercase conversion.
- Parses unsigned integer line values with overflow checking.
- Checks bulk string size before allocating the `String`.

Writer behavior:

- Simple string: `+...\r\n`
- Error: `-...\r\n`
- Bulk string: `$<size>\r\n<bytes>\r\n`
- Null bulk string: `$-1\r\n`
- Array header: `*<size>\r\n`

Parser limits:

- `MAX_ARRAY_ELEMENTS = 2048`
- `MAX_BULK_STRING_SIZE = 1024 * 1024`

Parser-level limit failures use the existing protocol error path in `RedisHandler`.

### `src/Server/RedisHandler.h`

Defines `RedisHandler`, a `Poco::Net::TCPServerConnection`.

Important fields:

- `IServer & server`
- `TCPServer & tcp_server`
- `connection_id`
- `selected_db`
- `has_selected_target`
- `selected_target`

`selected_target` contains:

- `database`
- `table`
- `default_column`

### `src/Server/RedisHandler.cpp`

Implements connection state and command dispatch.

Connection handling:

- Creates `ReadBufferFromPocoSocket`.
- Uses `AutoCanceledWriteBuffer<WriteBufferFromPocoSocket>` to avoid finalization/cancel assertions.
- Loops while the TCP server is open.
- Reads RESP commands through `RedisProtocol::readCommand`.
- Writes responses through `RedisProtocol` helpers.
- Calls `out->next` after each normal command response.
- Calls `out->finalize` on normal close paths such as `QUIT`.

Command dispatch:

- `PING`: returns `PONG` or echoes one argument as a bulk string.
- `QUIT`: returns `OK` and closes the connection.
- `SELECT`: parses a non-negative numeric DB index and resolves config.
- `GET`: validates arity and calls `RedisHandler::getKey`.
- `MGET`: validates non-empty arguments and calls `RedisHandler::getKeys`.
- Unknown commands return `ERR unknown command '<COMMAND>'`.

`SELECT` state:

- State is per connection.
- `selectDatabase` parses the DB number with `std::from_chars`.
- Reads `redis.db._<db>.database`, `redis.db._<db>.table`, and `redis.db._<db>.default_column`.
- Stores the target in `selected_target`.

Table resolution:

- `RedisHandler::getKey` and `RedisHandler::getKeys` call `DatabaseCatalog::instance().getTable`.
- The resulting `StoragePtr` is cast with `std::dynamic_pointer_cast<IKeyValueEntity>`.
- If the cast fails, the command returns `ERR target table does not support key-value lookup`.

Type and schema checks:

- Requires exactly one primary key column.
- Uses `IKeyValueEntity::getSampleBlock` to inspect key and value types.
- Requires value type `String`.
- Supports key type `String`.
- Supports key type `UInt64` by parsing Redis key bytes with `std::from_chars` into `UInt64`.
- Rejects other key types with `ERR only String or UInt64 key column is supported`.
- Rejects invalid `UInt64` text with `ERR invalid UInt64 key`.

Lookup:

- Builds `ColumnsWithTypeAndName` for the key column.
- Calls `IKeyValueEntity::getByKeys` once per `GET` or once per full `MGET`.
- Uses `found_map` to decide whether to return a bulk string or null bulk string.
- Requires `offsets` to be empty and row counts to match the requested key count.

Handler-level limits:

- `MAX_KEY_SIZE = 64 * 1024`
- `MAX_MGET_KEYS = 1024`
- Key size is checked before `UInt64` parsing and before `IKeyValueEntity::getByKeys`.
- `MGET` key count is checked before building ClickHouse key columns.

### `src/Server/RedisHandlerFactory.h` and `src/Server/RedisHandlerFactory.cpp`

Defines and implements `RedisHandlerFactory`.

Behavior:

- Inherits `TCPServerConnectionFactory`.
- Maintains an atomic connection id counter.
- Creates a `RedisHandler` for each accepted socket.
- Logs connection creation.
- Returns a dummy empty handler if `Poco::Net::NetException` indicates the client is already disconnected.

### `src/Server/ServerType.h` and `src/Server/ServerType.cpp`

Adds `REDIS` as a server type and maps `redis_port` to `ServerType::Type::REDIS`.

This lets Redis listener lifecycle follow the same broad pattern as other first-class protocol listeners.

### `programs/server/Server.cpp`

Registers the Redis listener when `redis_port` is configured and the server type allows `REDIS`.

### `programs/server/config.xml` and `programs/server/config.yaml.example`

Contain optional commented examples for:

- `redis_port`
- Redis DB to ClickHouse table mapping

The comments now describe current implementation scope and limitations.

## 6. Implementation stages

### 1. Architecture analysis

Goal: inspect ClickHouse protocol server architecture and existing key-value abstractions.

Files and notes:

- `project_notes/redis_memcached_kv_project_analysis.md`
- `src/Interpreters/IKeyValueEntity.h`
- `programs/server/Server.cpp`
- existing protocol handler factories

Implemented: analysis only.

Verification: repository inspection.

Limitations: no code yet.

### 2. Baseline benchmark

Goal: measure pre-implementation point lookup overhead through ClickHouse HTTP SQL and Redis reference.

Files and notes:

- `benchmark/kv_baseline/*`
- `project_notes/redis_memcached_kv_baseline_results_summary.md`

Implemented:

- Dataset utilities.
- ClickHouse HTTP SQL benchmark.
- Redis reference benchmark.

Representative preliminary baseline:

- 100k `String` keys.
- ClickHouse HTTP SQL around 1126 QPS at batch-size 1, concurrency 1.
- Redis reference around 48602 QPS at batch-size 1, concurrency 1.

Verification: benchmark run recorded in notes.

Limitations: preliminary, local, not final.

### 3. Redis TCP skeleton

Goal: add a minimal RESP/TCP endpoint.

Files changed:

- `src/Server/RedisProtocol.h`
- `src/Server/RedisProtocol.cpp`
- `src/Server/RedisHandler.h`
- `src/Server/RedisHandler.cpp`
- `src/Server/RedisHandlerFactory.h`
- `src/Server/RedisHandlerFactory.cpp`
- `src/Server/ServerType.h`
- `src/Server/ServerType.cpp`
- `programs/server/Server.cpp`

Implemented:

- `redis_port=9006`.
- `PING`.
- `QUIT`.
- unknown command errors.

Verification:

- `redis-cli -p 9006 PING` returned `PONG`.
- Raw RESP `PING` through `nc` returned `+PONG`.
- Unsupported `GET` returned unknown command.

Limitations:

- No `SELECT`, `GET`, or `MGET` yet.

### 4. `SELECT` plus config mapping

Goal: map Redis DB index to a ClickHouse target.

Files changed:

- `src/Server/RedisHandler.h`
- `src/Server/RedisHandler.cpp`
- config examples/status notes

Implemented:

- `SELECT <db>`.
- Numeric DB parsing.
- Per-connection selected target state.
- Config keys `redis.db._<n>.database`, `redis.db._<n>.table`, and `redis.db._<n>.default_column`.

Verification:

- `SELECT 0` returned `OK` for configured DB.
- `SELECT 999` returned unconfigured DB error.
- `SELECT abc` returned invalid DB index error.

Limitations:

- No `GET` or `MGET` yet.

### 5. `GET` through `IKeyValueEntity`

Goal: implement single-key lookup through ClickHouse direct key-value interface.

Files changed:

- `src/Server/RedisHandler.cpp`
- `src/Server/RedisHandler.h`

Implemented:

- `GET key`.
- Table resolution through `DatabaseCatalog`.
- Cast to `IKeyValueEntity`.
- Single `String` key support.
- `String` `default_column` response.
- Existing key as bulk string.
- Missing key as null bulk string.

Verification:

- Manual `redis-cli` checks for existing, missing, wrong arity, and liveness.
- Build checks passed after sandbox escalation when needed.

Limitations:

- No `MGET` yet.
- Only `String` key at this stage.

### 6. `MGET` through `IKeyValueEntity`

Goal: add batched lookup with one `getByKeys` call.

Files changed:

- `src/Server/RedisHandler.cpp`
- `src/Server/RedisHandler.h`

Implemented:

- `MGET key1 key2 ...`.
- Builds one key column containing all requested keys.
- Calls `IKeyValueEntity::getByKeys` once.
- Returns Redis array in input order.
- Missing keys become null bulk strings.

Verification:

- Manual `redis-cli` checks for all existing, mixed missing, wrong arity, before `SELECT`, and `GET` regression.

Limitations:

- Still `String` key only at this stage.

### 7. Integration tests

Goal: add regression coverage for basic Redis protocol behavior.

Files changed:

- `tests/integration/test_redis_protocol/test.py`
- `tests/integration/test_redis_protocol/configs/redis.xml`
- `tests/integration/test_redis_protocol/__init__.py`

Implemented:

- Cluster config with `redis_port=9006`.
- `kv_baseline` `EmbeddedRocksDB` table.
- Raw socket RESP helpers.
- Tests for `PING`, `QUIT`, `SELECT`, `GET`, `MGET`, errors, and liveness.

Verification:

- `py_compile` passed.
- Full integration runner availability later remained limited by missing `pytest`/Docker in the local environment.

### 8. Final 100k benchmark

Goal: compare ClickHouse HTTP SQL, ClickHouse Redis-compatible endpoint, and Redis raw reference on the same 100k `String` key dataset.

Files and notes:

- `benchmark/kv_baseline/bench_clickhouse_http.py`
- `benchmark/kv_baseline/bench_clickhouse_redis.py`
- `benchmark/kv_baseline/bench_redis_raw.py`
- `project_notes/redis_final_benchmark_results_summary.md`
- `project_notes/redis_final_benchmark_validity_review.md`

Implemented:

- Raw socket RESP benchmark for ClickHouse endpoint.
- Raw socket RESP benchmark for Redis reference.
- Final summary and validity review.

Verification:

- Result matrix reviewed for completeness.
- Script review found no definite script bug.

Limitations:

- Hot-cache local-loopback.
- One run per combination.
- QPS is per command/request.

### 9. `UInt64` key support

Goal: support numeric ClickHouse primary keys in the Redis endpoint.

Files changed:

- `src/Server/RedisHandler.cpp`

Implemented:

- Key-column construction for `TypeIndex::UInt64`.
- Redis key text parsed with `std::from_chars`.
- Invalid `UInt64` keys return Redis errors.
- `String` key support remains.

Verification:

- Manual checks on `bench.kv_test`.
- `GET 0`, `GET 65536`, and `MGET 0 65536 131072` returned expected values.
- Invalid keys `abc`, `-1`, and `18446744073709551616` returned errors.
- Build passed after sandbox escalation.

Limitations:

- Only `String` and `UInt64` key types.
- Only `String` value column.

### 10. 10M-row `bench.kv_test` benchmark

Goal: verify benchmark behavior on a larger existing `UInt64` key table.

Table:

```sql
CREATE TABLE bench.kv_test
(
    key UInt64,
    value String,
    extra1 UInt32,
    extra2 Float64
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

Results:

- HTTP SQL best QPS: 2763.65.
- Redis endpoint best QPS: 80652.74.
- p99 batch-size 1, concurrency 1: HTTP SQL 1.059 ms, Redis endpoint 0.028 ms.
- p99 batch-size 100, concurrency 32: HTTP SQL 52.755 ms, Redis endpoint 19.824 ms.

Verification:

- Results recorded in benchmark summary.

Limitations:

- Hot-cache local-loopback.
- Redis raw reference not included in the 10M table summary.

### 11. Defense demo / proof UI

Goal: make the request path difference understandable for a defense audience.

Files changed:

- `demo/redis_endpoint_demo.py`
- `demo/README.md`
- demo project notes

Implemented:

- Streamlit UI.
- SQL/HTTP and Redis endpoint comparison.
- Request path diagrams.
- Manual SQL and Redis consoles.
- `system.query_log` proof for SQL path.
- Static benchmark context.

Verification:

- `python3 -m py_compile demo/redis_endpoint_demo.py` passed.
- Streamlit started on `0.0.0.0:8501` during smoke check.
- Dependencies are Streamlit plus Python standard library.

Limitations:

- UI latency is illustrative, not benchmark data.
- SQL console is documented read-only but does not enforce `SELECT`-only.

### 12. Integration test expansion

Goal: cover `UInt64`, invalid keys, unsupported schemas, and liveness.

Files changed:

- `tests/integration/test_redis_protocol/test.py`
- `tests/integration/test_redis_protocol/configs/redis.xml`

Implemented DB mappings:

- DB `0`: `default.kv_baseline`, `String` key, `String` value.
- DB `1`: `default.kv_uint64`, `UInt64` key, `String` value.
- DB `2`: `default.kv_not_key_value`, non-`IKeyValueEntity`.
- DB `3`: `default.kv_uint32_key`, unsupported `UInt32` key.
- DB `4`: `default.kv_uint64_value`, unsupported `UInt64` value.

Verification:

- `py_compile` passed.
- Full integration tests not executed locally because `pytest` and Docker were unavailable.

Known risk:

- `EmbeddedRocksDB` with `UInt32` primary key in unsupported-key test may require adjustment if the real integration runner rejects the schema before the Redis endpoint can reject it.

### 13. Protective request limits

Goal: prevent unbounded memory use for malformed or huge Redis/RESP requests.

Files changed:

- `src/Server/RedisProtocol.cpp`
- `src/Server/RedisHandler.cpp`
- `tests/integration/test_redis_protocol/test.py`

Implemented:

- Parser-level array and bulk string limits.
- Handler-level key size and `MGET` key count limits.
- Integration tests for the limits.

Verification:

- `ninja -C build-new clickhouse-server-lib > build-new/redis_limits_verify_build.log 2>&1` passed.
- `py_compile` passed for integration tests.
- Full integration tests not executed locally because `pytest` and Docker were unavailable.

Limitations:

- No total command size limit.
- Limits are hardcoded MVP constants.

### 14. Config comments cleanup

Goal: align config examples with the current endpoint scope.

Files changed:

- `programs/server/config.xml`
- `programs/server/config.yaml.example`

Implemented:

- Removed stale "skeleton", `PING`/`QUIT` only, `SELECT` only, and future `GET`/`MGET` wording.
- Added read-only scope, supported commands, `IKeyValueEntity::getByKeys`, and non-Redis-replacement caveat.

Verification:

- Diff is comments only.
- No config values or runtime behavior changed.

### 15. Benchmark documentation update

Goal: make benchmark documentation suitable for thesis use.

Files changed:

- `project_notes/redis_final_benchmark_results_summary.md`
- `project_notes/redis_benchmark_docs_status.md`

Implemented:

- Research question framing.
- 100k `String` benchmark summary.
- 10M `UInt64` `bench.kv_test` summary.
- QPS per command/request clarification.
- `MGET` key throughput as derived metric.
- Caveats and careful conclusion.

Verification:

- Docs only.
- No benchmarks rerun.
- No scripts changed.

## 7. Issues encountered and fixes

| Stage | Issue | Symptom | Root cause | Fix | Verification | Source note file |
|---|---|---|---|---|---|---|
| Redis TCP skeleton | `WriteBuffer` finalization assertion | First `PING` response worked, then server terminated due to write buffer finalization assertion | Normal close/cancel path for `WriteBuffer` was not handled correctly | Use `AutoCanceledWriteBuffer<WriteBufferFromPocoSocket>` and call `finalize` on normal paths | `PING`, `QUIT`, and unsupported command checks no longer terminate server | `redis_handler_skeleton_status.md`, `redis_demo_implementation_issues.md` |
| `GET` implementation | Wrong build target | `ninja -C build clickhouse-server` failed with unknown target | Build tree exposes different targets | Use available target or `clickhouse-server-lib` in `build-new` | Targeted compile/build completed | `redis_get_implementation_issues.md` |
| `GET` and later builds | Sandbox `ccache` write failure | Build failed with `ccache: error: Read-only file system` | Sandbox prevented compiler cache writes | Rerun same build with escalated permissions | Build completed after escalation | `redis_get_implementation_issues.md`, `redis_uint64_key_implementation_issues.md`, `redis_limits_status.md` |
| `GET` verification | Stale server process | Redis checks reached different binaries over IPv4/IPv6 and config mismatch appeared | Two ClickHouse processes were running | Stop both and start one fresh `build-new` server with intended config | Manual Redis checks and server liveness passed | `redis_get_status.md` |
| `UInt64` support | Build target name | `ninja -C build-new clickhouse-server` was not valid | Build tree target names differ | Use `ninja -C build-new clickhouse-server-lib` | Server library compiled and linked | `redis_uint64_key_implementation_issues.md` |
| Integration tests | Full runner unavailable | Integration tests not executed locally | `pytest` and Docker unavailable | Record limitation; do not install or guess | `py_compile` passed; runner commands documented | `redis_integration_tests_completion_status.md`, `redis_limits_status.md` |
| Integration tests | Potential unsupported-key schema risk | `kv_uint32_key` may fail during setup in real runner | `EmbeddedRocksDB` may reject `UInt32` primary key schema before Redis endpoint test | If encountered, switch to another creatable unsupported schema | Not yet verified in full runner | `redis_integration_tests_completion_status.md` |
| Defense demo | Browser opened Redis port directly | Browser showed `-ERR protocol error` | Port `9006` speaks RESP/TCP, not HTTP | Use Streamlit on `8501`; use `redis-cli` or demo client for `9006` | Streamlit UI works; Redis commands work via RESP client | `redis_demo_implementation_issues.md` |
| Defense demo | Blank page or wrong localhost | UI not usable in browser | SSH/remote port forwarding confusion | Use Streamlit network URL, VS Code forwarding, or SSH tunnel | Streamlit loaded successfully | `redis_demo_implementation_issues.md` |
| Defense demo | Streamlit socket bind `PermissionError` | First smoke check failed | Sandbox permission restriction, not app code | Rerun with approved/escalated permission | Streamlit started on `0.0.0.0:8501` | `redis_demo_implementation_issues.md` |
| Demo verification | Python cache generated | `demo/__pycache__` appeared | `py_compile`/import checks generated bytecode | Remove cache before commit | `find` checks returned nothing | `redis_demo_implementation_issues.md` |
| Demo proof UI | `query_log` proof too broad | Proof filtered only by table name | Filter did not include configured database | Filter by configured database and table | Code inspection and query log output | `redis_demo_implementation_issues.md` |
| Demo proof UI | Redis DB hardcoded | `SELECT 0` assumed in parts of demo | Initial demo assumed DB `0` | Add configurable Redis DB and use it in command generation | No hardcoded runtime `SELECT 0` remains | `redis_demo_implementation_issues.md` |
| Documentation | Wording too broad | Text could imply production-wide performance or broad Redis compatibility | Initial wording exceeded endpoint scope | Replace with careful read-only/local-benchmark wording | Grep found no relevant misleading claims in current demo docs | `redis_demo_implementation_issues.md` |
| Protective limits | Parser and handler limits overlapped | `MGET` with 1024 keys had 1025 RESP array elements and could be rejected by parser before handler | RESP array count includes command name | Increase parser `MAX_ARRAY_ELEMENTS` to 2048, keep `MAX_MGET_KEYS` 1024 | Build passed; handler-level `MGET` limit is testable | conversation audit and `redis_limits_status.md` |
| Protective limits | Parser-level tests depend on current error path | Tests expect error response before close | Current `RedisHandler` writes `ERR Protocol error` then closes on parser exceptions | Document behavior and future adjustment | Status note records that tests may need relaxing if behavior changes | `redis_limits_status.md` |

No additional undocumented runtime issues were invented for this summary.

## 8. Tests

Integration test files:

- `tests/integration/test_redis_protocol/test.py`
- `tests/integration/test_redis_protocol/configs/redis.xml`

Test helpers:

- Raw socket RESP encoder.
- RESP response parser.
- `connect_redis`.
- `command`.
- `assert_error`.
- `assert_is_error`.
- `assert_error_contains`.
- `assert_server_accepts_new_connection`.

### Basic protocol

Covered:

- `PING`: `test_ping`.
- `QUIT`: `test_quit`.
- unknown command: `test_server_survives_errors`.
- parser/protocol limit errors: `test_resp_array_too_large`, `test_resp_bulk_string_too_large`.

Not fully covered:

- generic malformed RESP beyond limit cases.
- `PING` wrong arity.
- `QUIT` wrong arity.

### `SELECT`

Covered:

- valid `SELECT`: `test_select_valid_db`.
- unconfigured DB: `test_select_invalid_db`.
- invalid DB index: `test_select_invalid_index`.

Not explicitly covered:

- `SELECT` wrong arity.

### `String` `GET`/`MGET`

Covered:

- `GET` existing: `test_get_existing_key`.
- `GET` missing: `test_get_missing_key`.
- `GET` wrong arity: `test_get_wrong_arity`.
- `GET` before `SELECT`: `test_get_before_select`.
- `MGET` existing: `test_mget_existing_keys`.
- `MGET` mixed missing: `test_mget_mixed_existing_missing`.
- `MGET` wrong arity: `test_mget_wrong_arity`.
- `MGET` before `SELECT`: `test_mget_before_select`.

### `UInt64`

Covered:

- `GET` existing key `0`: `test_uint64_get_existing_key`.
- `GET` existing key `65536`: `test_uint64_get_another_existing_key`.
- `GET` missing: `test_uint64_get_missing_key`.
- `MGET` existing: `test_uint64_mget_existing_keys`.
- `MGET` mixed missing: `test_uint64_mget_mixed_existing_missing`.
- invalid key `abc`: `test_uint64_get_invalid_key`, `test_uint64_mget_invalid_key`.
- invalid key `-1`: same tests.
- overflow key `18446744073709551616`: same tests.

### Unsupported schemas

Covered for both `GET` and `MGET`:

- non-`IKeyValueEntity` table: `kv_not_key_value`, `test_get_non_key_value_table_error`, `test_mget_non_key_value_table_error`.
- unsupported key type: `kv_uint32_key`, `test_get_unsupported_key_type_error`, `test_mget_unsupported_key_type_error`.
- unsupported value/default column type: `kv_uint64_value`, `test_get_unsupported_value_type_error`, `test_mget_unsupported_value_type_error`.

### Protective limits

Covered:

- `GET` key too large: `test_get_key_too_large`.
- `MGET` key too large: `test_mget_key_too_large`.
- `MGET` too many keys: `test_mget_too_many_keys`.
- RESP array too large: `test_resp_array_too_large`.
- bulk string too large: `test_resp_bulk_string_too_large`.

### Liveness

Covered:

- after command errors: `test_server_survives_errors`.
- after invalid `UInt64` errors: `test_uint64_server_survives_invalid_key_errors`.
- after unsupported schema errors: `PING` in each unsupported schema test.
- after handler-level limit errors: `PING` in each handler-level limit test.
- after parser-level/protocol limit errors: new connection `PING` in parser-level limit tests.

### Verification status

- `python3 -m py_compile tests/integration/test_redis_protocol/test.py` passed.
- C++ build passed for the protective limits stage.
- Generated `__pycache__` was removed after checks.
- Full integration tests were not executed locally because `pytest` and Docker were unavailable.

Known runtime risk:

- The unsupported key type test currently uses `EmbeddedRocksDB` with `UInt32` primary key. If the real runner rejects this schema during setup, use another unsupported key schema that can be created but is rejected by `RedisHandler`.

## 9. Protective request limits

Final limits:

Parser-level:

- `MAX_ARRAY_ELEMENTS = 2048`
- `MAX_BULK_STRING_SIZE = 1 MiB`

Handler-level:

- `MAX_KEY_SIZE = 64 KiB`
- `MAX_MGET_KEYS = 1024`

Behavior:

- Bulk string size is checked before `String::resize`, avoiding oversized allocation.
- RESP array size is checked before reserving argument vector capacity.
- `MGET` key count is checked before building ClickHouse key columns.
- Key size is checked before `UInt64` parsing and before `IKeyValueEntity::getByKeys`.
- Handler-level errors return Redis errors and keep the connection usable.
- Parser-level errors use the existing protocol error path: `RedisHandler` writes `ERR Protocol error`, finalizes, and closes the connection.

Not implemented:

- Total command size limit.
- Config-based limit settings.

Reason total command size is future work:

- The parser currently reads RESP fields structurally and does not track aggregate command bytes.
- The current combination of array element, bulk string, key size, and `MGET` count limits provides meaningful MVP protection.

## 10. Benchmarks

Benchmark docs:

- `project_notes/redis_final_benchmark_results_summary.md`
- `project_notes/redis_final_benchmark_validity_review.md`
- `project_notes/redis_benchmark_docs_status.md`
- `benchmark/kv_baseline/README.md`

### 100k `String` key benchmark

Dataset:

- 100000 keys.
- Key type: `String`.
- Key format: `key_000000000`.
- Value type: `String`.
- Value size: 64 bytes.
- ClickHouse table engine: `EmbeddedRocksDB`.

Compared paths:

- ClickHouse HTTP SQL.
- ClickHouse Redis-compatible endpoint with raw socket RESP client.
- Redis raw reference with raw socket RESP client.

Key results:

- HTTP SQL best QPS: about 2827.
- ClickHouse Redis-compatible endpoint best QPS: about 84491.
- Redis raw reference best QPS: about 90535.

p99 at batch-size 1, concurrency 1:

- HTTP SQL: about 1.021 ms.
- ClickHouse Redis-compatible endpoint: about 0.021 ms.
- Redis raw reference: about 0.015 ms.

### 10M `UInt64` `bench.kv_test` benchmark

Table:

- `bench.kv_test`.
- Rows: 10M.
- Key: `UInt64`.
- Value: `String`.
- Extra columns: `extra1 UInt32`, `extra2 Float64`.
- Engine: `EmbeddedRocksDB`.
- Primary key: `key`.

Results:

- HTTP SQL best QPS: 2763.65.
- Redis endpoint best QPS: 80652.74.
- p99 batch-size 1, concurrency 1:
  - HTTP SQL: 1.059 ms.
  - Redis endpoint: 0.028 ms.
- p99 batch-size 100, concurrency 32:
  - HTTP SQL: 52.755 ms.
  - Redis endpoint: 19.824 ms.

### Benchmark interpretation rules

- QPS is per command/request.
- `GET` is one command.
- `MGET` is one command containing multiple keys.
- `MGET` approximate key throughput is request QPS multiplied by batch size.
- Latency remains per request/command, not per key.
- Results are hot-cache local-loopback microbenchmarks.
- Redis raw is an external reference baseline.
- Results do not prove production-wide performance.
- Do not claim ClickHouse is faster than Redis.

## 11. Demo / proof UI

Files:

- `demo/redis_endpoint_demo.py`
- `demo/README.md`

Purpose:

- Explain and demonstrate the difference between the ClickHouse HTTP SQL path and the Redis-compatible endpoint path.
- Show actual requests, matching results, illustrative client-side latency, request path diagrams, and live proof checks.

Implemented tabs:

- Overview.
- Compare `GET`/`MGET`.
- Request Path.
- Manual Console.
- Proof / Live Verification.
- Benchmark Context.

Features:

- Shows configured HTTP endpoint and Redis-compatible endpoint.
- Shows selected database, table, key column, value column, key type, and Redis DB index.
- Generates and displays the actual SQL query.
- Generates and displays the Redis command sequence.
- Sends Redis commands over one TCP connection so `SELECT` state is preserved.
- Compares SQL and Redis results.
- Preserves `MGET` requested-key order in comparison by converting SQL rows to a dictionary and reconstructing by requested key order.
- Displays illustrative client-side latency.
- Shows request path diagrams.
- Provides copyable commands for `ss`, `pgrep`, `system.query_log`, and log tailing.
- Provides a button to check recent SQL `system.query_log` entries.
- Explains that SQL path requests should appear in `system.query_log`, while Redis `GET`/`MGET` are handled by `RedisHandler` and should not appear as SQL queries.

Important limitation:

- UI latency is illustrative and is not a benchmark replacement.

## 12. Limitations

Final project limitations:

- Not full Redis.
- Not a Redis replacement.
- Not a general Redis-compatible server.
- Read-only lookup endpoint.
- Only `GET`/`MGET` data access is implemented.
- Only `PING`, `QUIT`, `SELECT`, `GET`, and `MGET` are supported.
- Only `IKeyValueEntity`-compatible tables are supported.
- No arbitrary `MergeTree` support through this endpoint.
- Single key column only.
- `String` and `UInt64` keys only.
- `String` `default_column` value only.
- No composite keys.
- No `SET`, `DEL`, `TTL`, `AUTH`, Pub/Sub, Lua, cluster, streams, sorted sets, or hash commands.
- No auth/security design in this MVP.
- No total command size limit.
- No config-based limit settings.
- Full integration tests still need a real ClickHouse integration environment with `pytest` and Docker.
- Benchmark results are local hot-cache microbenchmarks, not production-wide proof.
- HTTP benchmark likely includes per-request client/HTTP connection overhead because the Python script does not explicitly reuse persistent HTTP connections.
- Benchmark response validation checks shape more than exact returned values.

## 13. Future work

Potential future work:

- Config-based request limits.
- Total command size tracking.
- More value types and explicit serialization policies.
- Composite key support.
- `HGET` and `HMGET`.
- Memcached text protocol.
- Auth/security integration with ClickHouse users.
- Quotas and readonly access controls.
- Persistent-connection HTTP benchmark variant.
- Repeated benchmark runs and medians/confidence intervals.
- More strict benchmark response validation.
- More cache-state documentation: warm cache, cold cache, RocksDB cache, OS page cache.
- Additional `IKeyValueEntity` backend evaluation, such as dictionaries, if feasible.
- CI integration test run in full ClickHouse environment.
- Better telemetry/logging/metrics for Redis endpoint requests.
- Response size limits.
- Production-grade client compatibility testing with common Redis clients.

## 14. Thesis writing guide

### Chapter 1: Context and problem

Use:

- `project_notes/redis_memcached_kv_research_question.md`
- `project_notes/redis_memcached_kv_project_analysis.md`
- `project_notes/redis_memcached_kv_baseline_results_summary.md`
- `project_notes/redis_memcached_kv_report_outline.md`

Key message:

- ClickHouse can store prepared analytical/key-value datasets, but the SQL/HTTP path adds overhead for simple point lookups.
- The thesis studies whether a narrow read-only key-value protocol path can reduce this overhead.

### Chapter 2: ClickHouse internals and key-value data marts

Use:

- `src/Interpreters/IKeyValueEntity.h`
- `src/Storages/RocksDB/StorageEmbeddedRocksDB.cpp`
- `src/Dictionaries/IDictionary.h`
- `src/Storages/StorageKeeperMap.cpp`
- `src/Storages/StorageRedis.cpp`
- `programs/server/Server.cpp`
- `src/Server/ServerType.h`
- `src/Server/ServerType.cpp`
- `project_notes/redis_memcached_kv_project_analysis.md`

Key message:

- The endpoint reuses existing ClickHouse direct key-value lookup abstractions and protocol server infrastructure.

### Chapter 3: Architecture of Redis-compatible endpoint

Use:

- `src/Server/RedisProtocol.h`
- `src/Server/RedisProtocol.cpp`
- `src/Server/RedisHandler.h`
- `src/Server/RedisHandler.cpp`
- `src/Server/RedisHandlerFactory.h`
- `src/Server/RedisHandlerFactory.cpp`
- `programs/server/config.xml`
- `programs/server/config.yaml.example`
- `project_notes/redis_handler_skeleton_status.md`
- `project_notes/redis_select_mapping_status.md`

Key message:

- The Redis-compatible path is a TCP/RESP path that resolves a configured target and directly calls `IKeyValueEntity::getByKeys`.

### Chapter 4: Implementation

Use:

- `project_notes/redis_get_status.md`
- `project_notes/redis_mget_status.md`
- `project_notes/redis_uint64_key_support_status.md`
- `project_notes/redis_limits_status.md`
- `project_notes/redis_config_comments_status.md`

Key message:

- Implementation evolved from skeleton to `SELECT`, `GET`, `MGET`, `UInt64`, protective limits, and documentation cleanup.

### Chapter 5: Testing and robustness

Use:

- `tests/integration/test_redis_protocol/test.py`
- `tests/integration/test_redis_protocol/configs/redis.xml`
- `project_notes/redis_integration_tests_completion_status.md`
- `project_notes/redis_limits_status.md`
- `project_notes/redis_demo_implementation_issues.md`
- issue notes in `project_notes/*issues*.md`

Key message:

- Tests cover protocol basics, `SELECT`, `String` and `UInt64` lookups, unsupported schemas, protective limits, and server liveness. Full integration runner still must be executed in an environment with `pytest` and Docker.

### Chapter 6: Benchmarks and evaluation

Use:

- `benchmark/kv_baseline/README.md`
- `benchmark/kv_baseline/bench_clickhouse_http.py`
- `benchmark/kv_baseline/bench_clickhouse_redis.py`
- `benchmark/kv_baseline/bench_redis_raw.py`
- `project_notes/redis_final_benchmark_results_summary.md`
- `project_notes/redis_final_benchmark_validity_review.md`
- `project_notes/redis_benchmark_docs_status.md`

Key message:

- The endpoint substantially reduces measured overhead versus the tested ClickHouse HTTP SQL path on local hot-cache benchmarks. Redis is only an external reference baseline.

### Chapter 7: Limitations and future work

Use:

- `project_notes/redis_memcached_kv_research_question.md`
- `project_notes/redis_final_benchmark_validity_review.md`
- `project_notes/redis_limits_status.md`
- this summary's limitations and future work sections.

Key message:

- The result validates a narrow architecture, not full Redis compatibility or production-wide performance.

## 15. Important wording rules

Use:

- Redis-compatible read-only endpoint.
- prepared key-value tables.
- `IKeyValueEntity::getByKeys`.
- removes much of SQL/HTTP overhead.
- supported point lookups.
- hot-cache local-loopback microbenchmark.
- external Redis reference baseline.
- ClickHouse Redis-compatible endpoint.
- ClickHouse HTTP SQL path.

Avoid:

- ClickHouse is faster than Redis.
- full Redis.
- Redis replacement.
- production-ready.
- arbitrary ClickHouse tables.
- general Redis-compatible server.
- drop-in Redis.
- production performance proven.

Recommended final conclusion:

> The experiments support the claim that a specialized Redis-compatible read-only endpoint can substantially reduce SQL/HTTP overhead for supported point lookups over prepared ClickHouse key-value tables. This is shown both on the 100k `String` key benchmark and on the 10M-row `UInt64` `bench.kv_test` table. The result does not prove production-wide performance and does not imply that ClickHouse is generally faster than Redis.
