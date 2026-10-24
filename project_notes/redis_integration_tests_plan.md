# Redis-Compatible Endpoint Integration Test Plan

## Test Location

ClickHouse integration tests live under `tests/integration/`.

The general pattern is:

- one directory per test, for example `tests/integration/test_storage_redis/`;
- an empty `__init__.py`;
- a `test.py` file with `pytest` tests;
- optional config fragments under `configs/`;
- a `ClickHouseCluster(__file__)` object and one or more `cluster.add_instance` calls.

The integration test README is `tests/integration/README.md`. Local selective runs use:

```bash
python -m ci.praktika run "integration" --test test_redis_protocol
```

or, for direct pytest-style runs in a prepared environment:

```bash
pytest tests/integration/test_redis_protocol
```

## Good Existing References

### Custom Server Config

- `tests/integration/test_rocksdb_options/test.py`
- `tests/integration/test_rocksdb_options/configs/rocksdb.xml`

This test uses:

```python
node = cluster.add_instance(
    "node", main_configs=["configs/rocksdb.xml"], stay_alive=True
)
```

The config fragment is copied into `/etc/clickhouse-server/config.d/`.

### Opening Extra Ports

- `tests/integration/test_composable_protocols/test.py`
- `tests/integration/test_composable_protocols/configs/config.xml`

This test defines extra protocol listeners in a config fragment and connects to them through `server.ip_address`.

The shared integration config `tests/integration/helpers/0_common_instance_config.xml` sets:

```xml
<listen_host>0.0.0.0</listen_host>
```

So a `redis_port` listener configured in a test config should be reachable at `node.ip_address:9006` from the test runner.

### Python Clients

Redis client dependency is already available in the integration environment:

- `tests/integration/README.md` lists `redis` in the Python dependencies.
- `tests/integration/test_storage_redis/test.py` imports `redis`.
- `tests/integration/test_table_function_redis/test.py` imports `redis`.
- `tests/integration/test_dictionaries_redis/test_long.py` imports `redis`.

Those tests use `with_redis=True`, which starts an external Redis container. The new endpoint test should not use `with_redis=True`, because it tests ClickHouse's own Redis-compatible listener.

### Raw Socket Clients

Good raw socket references:

- `tests/integration/test_http_header_limits/test.py`
- `tests/integration/test_tcp_hello_string_limits/test.py`
- `tests/integration/test_composable_protocols/test.py`

These tests show direct `socket.socket`, `connect`, `sendall`, and `recv` helpers against ClickHouse protocol ports.

### Creating Tables And Inserting Data

Good references:

- `tests/integration/test_rocksdb_options/test.py` for `EmbeddedRocksDB`.
- `tests/integration/test_storage_redis/test.py` and `tests/integration/test_table_function_redis/test.py` for Python test setup through `node.query`.

The fixture should use `node.query` with a multiquery string to drop, create, and insert into the `EmbeddedRocksDB` test table.

## Proposed New Test Directory

```text
tests/integration/test_redis_protocol/
```

## Proposed Files

```text
tests/integration/test_redis_protocol/__init__.py
tests/integration/test_redis_protocol/test.py
tests/integration/test_redis_protocol/configs/redis.xml
```

`configs/redis.xml` should configure ClickHouse's Redis-compatible endpoint and DB mapping.

Proposed config fragment:

```xml
<?xml version="1.0" encoding="utf-8"?>
<clickhouse>
    <redis_port>9006</redis_port>
    <redis>
        <db>
            <_0>
                <database>default</database>
                <table>kv_baseline</table>
                <default_column>value</default_column>
            </_0>
        </db>
    </redis>
</clickhouse>
```

## Required Test Setup

In `test.py`:

```python
import socket
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/redis.xml"])
```

Use a module-scoped fixture:

```python
@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        prepare_table()
        yield cluster
    finally:
        cluster.shutdown()
```

The table setup should:

- start ClickHouse with `redis_port=9006`;
- configure `redis.db._0.database=default`;
- configure `redis.db._0.table=kv_baseline`;
- configure `redis.db._0.default_column=value`;
- create `default.kv_baseline`;
- insert deterministic test keys.

Proposed SQL:

```sql
DROP TABLE IF EXISTS default.kv_baseline;
CREATE TABLE default.kv_baseline
(
    key String,
    value String
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key;

INSERT INTO default.kv_baseline VALUES
    ('key_000000001', 'value_000000001'),
    ('key_000000002', 'value_000000002'),
    ('key_000000003', 'value_000000003');
```

## Client Choice

Use a raw socket client for this test.

`redis-py` is already available in the integration environment and is used by existing tests, so it is a viable fallback. Raw socket is still preferable here because:

- it avoids starting or depending on an external Redis service;
- it asserts the exact RESP wire protocol;
- it makes Redis null bulk strings explicit as `$-1\r\n`;
- it keeps per-connection `SELECT` state obvious;
- it avoids client-side conversions of missing values to `None`.

The helper should encode commands as RESP arrays of bulk strings:

```python
def encode_command(*parts):
    result = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        if isinstance(part, str):
            part = part.encode()
        result.append(f"${len(part)}\r\n".encode())
        result.append(part)
        result.append(b"\r\n")
    return b"".join(result)
```

Use a single connection when command state matters:

```python
response = redis_request(
    encode_command("SELECT", "0") +
    encode_command("GET", "key_000000001")
)
```

The connection helper can connect to:

```python
(node.ip_address, 9006)
```

with a short timeout.

## Expected RESP Assertions

Expected raw responses:

- `PING`: `+PONG\r\n`
- `QUIT`: `+OK\r\n`
- `SELECT 0`: `+OK\r\n`
- `GET key`: `$15\r\nvalue_000000001\r\n`
- missing `GET`: `$-1\r\n`
- `MGET` with three values: `*3\r\n$15\r\nvalue_000000001\r\n$15\r\nvalue_000000002\r\n$15\r\nvalue_000000003\r\n`
- mixed `MGET`: `*3\r\n$15\r\nvalue_000000001\r\n$-1\r\n$15\r\nvalue_000000003\r\n`
- errors start with `-ERR ...\r\n`.

## Test Cases

### `test_ping`

Send:

```text
PING
```

Assert:

```text
+PONG\r\n
```

### `test_select_valid_db`

Send:

```text
SELECT 0
```

Assert:

```text
+OK\r\n
```

### `test_select_invalid_db`

Send `SELECT 1` or `SELECT abc`.

Assert an error:

- `-ERR Redis DB 1 is not configured\r\n` for unmapped DB;
- `-ERR invalid DB index\r\n` for non-numeric DB.

Prefer covering both if the test stays small.

### `test_get_existing_key`

Send `SELECT 0` and `GET key_000000001` on the same connection.

Assert:

```text
+OK\r\n$15\r\nvalue_000000001\r\n
```

### `test_get_missing_key`

Send `SELECT 0` and `GET missing_key` on the same connection.

Assert:

```text
+OK\r\n$-1\r\n
```

### `test_get_wrong_arity`

Send `GET` with no arguments and `GET a b`.

Assert:

```text
-ERR wrong number of arguments for 'get' command\r\n
```

### `test_get_before_select`

Send `GET key_000000001` on a new connection without `SELECT`.

Assert:

```text
-ERR no Redis DB selected\r\n
```

### `test_mget_existing_keys`

Send `SELECT 0` and:

```text
MGET key_000000001 key_000000002 key_000000003
```

Assert a three-element RESP array containing the three bulk strings in input order.

### `test_mget_mixed_existing_missing`

Send `SELECT 0` and:

```text
MGET key_000000001 missing_key key_000000003
```

Assert:

```text
*3\r\n$15\r\nvalue_000000001\r\n$-1\r\n$15\r\nvalue_000000003\r\n
```

### `test_mget_wrong_arity`

Send `MGET` with no arguments.

Assert:

```text
-ERR wrong number of arguments for 'mget' command\r\n
```

### `test_mget_before_select`

Send `MGET key_without_select` on a new connection without `SELECT`.

Assert:

```text
-ERR no Redis DB selected\r\n
```

### `test_server_survives_errors`

Run several error commands, then assert:

- `node.query("SELECT 1") == "1\n"`;
- a fresh Redis connection can still `PING`;
- optionally `SELECT 0` and `GET key_000000001` still work.

## Local Run

Recommended local run through Praktika:

```bash
python -m ci.praktika run "integration" --test test_redis_protocol
```

Direct run in a prepared integration environment:

```bash
pytest tests/integration/test_redis_protocol
```

When using a local `build-new` binary, set the integration test binary path if needed:

```bash
CLICKHOUSE_TESTS_SERVER_BIN_PATH=./build-new/programs/clickhouse \
CLICKHOUSE_TESTS_CLIENT_BIN_PATH=./build-new/programs/clickhouse \
pytest tests/integration/test_redis_protocol
```

## Risks

- Port conflicts: fixed `9006` inside the ClickHouse container should be fine, but exposing `9006` to the host would conflict with local manual servers. The test should connect to `node.ip_address:9006` and avoid host port publishing.
- `redis-cli` / `redis-py` dependency: avoid `redis-cli`; use raw Python sockets. `redis-py` is available but not needed.
- Selected DB state is per connection: tests must send `SELECT 0` on the same socket as `GET` or `MGET`.
- Missing key display differs between clients: raw RESP avoids ambiguity by asserting `$-1\r\n` directly.
- Startup readiness currently waits for native TCP port `9000`, not Redis port `9006`; the first raw socket helper should retry briefly or fail with a clear connection error.
- `EmbeddedRocksDB` stores data on disk inside the test container; use deterministic table names and `DROP TABLE IF EXISTS` during setup.
- If response helpers read until socket close, `PING`, `GET`, and `MGET` connections may hang because the server keeps the connection open. The raw client should read exactly one RESP value or send `QUIT` after the command batch.
