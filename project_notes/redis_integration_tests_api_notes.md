# Redis Integration Test API Notes

## Cluster Definition Style

Integration tests usually define a module-level cluster and node:

```python
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/redis.xml"])
```

The common fixture style is:

```python
@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()
```

Examples:

- `tests/integration/test_mysql_protocol/test.py`
- `tests/integration/test_postgresql_protocol/test.py`
- `tests/integration/test_rocksdb_options/test.py`
- `tests/integration/test_tcp_hello_string_limits/test.py`

Protocol tests usually define the cluster at module scope and use a module-scoped fixture named `started_cluster`.

## Adding A Config File To A Node

Use `main_configs` to copy config fragments into the ClickHouse server config directory:

```python
node = cluster.add_instance(
    "node",
    main_configs=["configs/redis.xml"],
)
```

References:

- `tests/integration/test_mysql_protocol/test.py` adds `configs/mysql.xml`.
- `tests/integration/test_postgresql_protocol/test.py` adds `configs/postgresql.xml`.
- `tests/integration/test_rocksdb_options/test.py` adds `configs/rocksdb.xml`.

Small protocol config fragments look like:

```xml
<clickhouse>
    <mysql_port>9001</mysql_port>
</clickhouse>
```

or:

```xml
<clickhouse>
    <postgresql_port>5433</postgresql_port>
</clickhouse>
```

For Redis, the planned fragment should be:

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

## Using `redis_port=9006`

Do not use `with_redis=True` for this test. That helper starts an external Redis container and is used by tests such as:

- `tests/integration/test_storage_redis/test.py`
- `tests/integration/test_table_function_redis/test.py`
- `tests/integration/test_dictionaries_redis/test_long.py`

The Redis-compatible endpoint test should connect to ClickHouse itself.

The shared integration config has:

```xml
<listen_host>0.0.0.0</listen_host>
```

So a configured `<redis_port>9006</redis_port>` should be reachable from the pytest process at:

```python
(node.ip_address, 9006)
```

The test should not publish `9006` to the host. Connecting to the container IP avoids conflicts with local manual servers.

`cluster.start` waits for native TCP port `9000`, not necessarily the Redis-compatible port. If the Redis listener opens slightly later, either:

- wait for a log line once the exact Redis startup log text is known; or
- make the raw socket helper retry connecting to `node.ip_address:9006` briefly.

`test_postgresql_protocol` uses this pattern for the PostgreSQL compatibility port:

```python
cluster.instances["node"].wait_for_log_line("PostgreSQL compatibility protocol")
```

## SQL Setup

Use `node.query` for table setup. Existing tests commonly run multi-statement SQL through `node.query`, for example `test_rocksdb_options`.

Recommended fixture setup:

```python
def prepare_table():
    node.query(
        """
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
        """
    )
```

Then call `prepare_table` after `cluster.start` and before yielding the fixture.

For a survival check after protocol errors:

```python
assert node.query("SELECT 1") == "1\n"
```

## Socket And Client Patterns

Raw socket references:

- `tests/integration/test_tcp_hello_string_limits/test.py`
- `tests/integration/test_http_header_limits/test.py`
- `tests/integration/test_composable_protocols/test.py`

Typical style:

```python
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(timeout)
sock.connect((node.ip_address, port))
sock.sendall(payload)
chunk = sock.recv(4096)
sock.close()
```

External Python client references:

- `test_mysql_protocol` uses MySQL clients and `pymysql`.
- `test_postgresql_protocol` uses `psycopg` / `psycopg2`.
- Redis storage and dictionary tests import `redis`.

The integration environment already has the `redis` Python dependency, but raw sockets are recommended for this endpoint because the test should assert RESP wire output exactly.

## Writing Raw RESP Commands

Helper to encode a Redis command:

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

Example:

```python
payload = encode_command("SELECT", "0") + encode_command("GET", "key_000000001")
```

Important: `SELECT` state is per connection. `SELECT 0` and `GET` / `MGET` must be sent over the same socket when testing successful key lookup.

## Reading Raw RESP Responses

Avoid helpers that read until socket close. The Redis-compatible endpoint keeps the connection open after most commands, so tests should parse exactly one RESP value at a time.

Suggested parser:

```python
def read_line(sock):
    data = b""
    while not data.endswith(b"\r\n"):
        chunk = sock.recv(1)
        if not chunk:
            raise AssertionError("connection closed")
        data += chunk
    return data[:-2]

def read_resp(sock):
    prefix = sock.recv(1)
    if prefix == b"+":
        return ("simple", read_line(sock))
    if prefix == b"-":
        return ("error", read_line(sock))
    if prefix == b"$":
        size = int(read_line(sock))
        if size == -1:
            return ("bulk", None)
        value = read_exact(sock, size)
        assert read_line(sock) == b""
        return ("bulk", value)
    if prefix == b"*":
        size = int(read_line(sock))
        return ("array", [read_resp(sock) for _ in range(size)])
    raise AssertionError(f"unexpected RESP prefix: {prefix!r}")
```

`read_exact` can follow the style from `test_tcp_hello_string_limits`:

```python
def read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise AssertionError("connection closed while reading bytes")
        buf += chunk
    return buf
```

A request helper can keep one connection open for multiple responses:

```python
def redis_exchange(*commands):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        sock.connect((node.ip_address, 9006))
        sock.sendall(b"".join(commands))
        return [read_resp(sock) for _ in commands]
```

## Assertions For Redis Responses

Represent parsed responses as tuples:

### Simple Strings

`PING` response:

```python
assert redis_exchange(encode_command("PING")) == [("simple", b"PONG")]
```

`SELECT 0` response:

```python
assert redis_exchange(encode_command("SELECT", "0")) == [("simple", b"OK")]
```

### Errors

Wrong `GET` arity:

```python
assert redis_exchange(encode_command("GET")) == [
    ("error", b"ERR wrong number of arguments for 'get' command")
]
```

`GET` before `SELECT`:

```python
assert redis_exchange(encode_command("GET", "key_000000001")) == [
    ("error", b"ERR no Redis DB selected")
]
```

### Bulk Strings

Successful `GET`:

```python
assert redis_exchange(
    encode_command("SELECT", "0"),
    encode_command("GET", "key_000000001"),
) == [
    ("simple", b"OK"),
    ("bulk", b"value_000000001"),
]
```

### Null Bulk Strings

Missing `GET`:

```python
assert redis_exchange(
    encode_command("SELECT", "0"),
    encode_command("GET", "missing_key"),
) == [
    ("simple", b"OK"),
    ("bulk", None),
]
```

### Arrays

Successful `MGET`:

```python
assert redis_exchange(
    encode_command("SELECT", "0"),
    encode_command("MGET", "key_000000001", "key_000000002", "key_000000003"),
) == [
    ("simple", b"OK"),
    ("array", [
        ("bulk", b"value_000000001"),
        ("bulk", b"value_000000002"),
        ("bulk", b"value_000000003"),
    ]),
]
```

Mixed hit/miss `MGET`:

```python
assert redis_exchange(
    encode_command("SELECT", "0"),
    encode_command("MGET", "key_000000001", "missing_key", "key_000000003"),
) == [
    ("simple", b"OK"),
    ("array", [
        ("bulk", b"value_000000001"),
        ("bulk", None),
        ("bulk", b"value_000000003"),
    ]),
]
```

## Recommended Approach

Create `tests/integration/test_redis_protocol/` with:

- `__init__.py`;
- `test.py`;
- `configs/redis.xml`.

Use:

```python
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/redis.xml"])
```

Use `node.query` in the module fixture to create and populate the `EmbeddedRocksDB` table.

Use raw socket RESP helpers instead of `redis-py`. This keeps the tests dependency-light at runtime, avoids any external Redis service, validates ClickHouse's wire protocol directly, and makes null bulk strings and per-connection `SELECT` state explicit.

Connect to `node.ip_address:9006`, not `localhost`, so the test targets the ClickHouse container directly and avoids host port conflicts.

Keep each test independent by using a fresh socket. For commands that require selected DB state, send `SELECT 0` and the tested command in the same socket exchange.
