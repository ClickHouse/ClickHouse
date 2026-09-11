# Redis Endpoint Demo UI Plan

## Goal

Create a small local demo UI that shows two access paths to the same ClickHouse data:

- Ordinary ClickHouse HTTP SQL query.
- Redis-compatible ClickHouse endpoint using `SELECT` plus `GET`/`MGET`.

The demo should show the returned values and measured latency side by side.

## Why This Helps The Defense

- Visualizes the difference between the SQL path and the Redis-compatible endpoint path.
- Shows that both paths read the same ClickHouse data.
- Shows result equality and latency side by side.
- Helps explain the project to reviewers who are not deep in ClickHouse internals.
- Makes `GET`/`MGET` behavior concrete without requiring reviewers to read terminal commands.

## Proposed Files

- `demo/redis_endpoint_demo.py`
- `demo/README.md`

## Proposed UI Technology

Use Streamlit for the local UI.

If Streamlit is missing, `demo/README.md` should explain:

```bash
python3 -m pip install --user --break-system-packages streamlit
```

The demo itself should avoid other external dependencies.

## Inputs

The UI should expose these inputs:

- ClickHouse HTTP host, default `127.0.0.1`
- ClickHouse HTTP port, default `8123`
- Redis endpoint host, default `127.0.0.1`
- Redis endpoint port, default `9006`
- Database, default `bench`
- Table, default `kv_test`
- Key column, default `key`
- Value column, default `value`
- Key type: `String` or `UInt64`
- Single key, default `0`
- Comma-separated `MGET` keys, default `0,65536,131072`

## `GET` Demo

The SQL path should run:

```sql
SELECT <value_column>
FROM <database>.<table>
WHERE <key_column> = <key>
LIMIT 1
```

The Redis endpoint path should run:

```text
SELECT 0
GET <key>
```

The demo should measure both paths with `time.perf_counter`.

The UI should show:

- SQL query.
- SQL result.
- Redis commands.
- Redis result.
- SQL latency.
- Redis endpoint latency.
- Whether the results match.
- Any error returned by either path.

## `MGET` Demo

The SQL path should run:

```sql
SELECT <key_column>, <value_column>
FROM <database>.<table>
WHERE <key_column> IN (...)
```

The Redis endpoint path should run:

```text
SELECT 0
MGET key1 key2 ...
```

The UI should show:

- SQL query.
- SQL results.
- Redis commands.
- Redis results.
- SQL latency.
- Redis endpoint latency.
- Per-key comparison in the original requested key order.

SQL results should be reordered on the client side to match the input key order before comparison, because SQL `IN` does not guarantee result order.

## Implementation Constraints

- Use Python standard library for ClickHouse HTTP requests if practical, for example `urllib.request`.
- Use Python standard library raw sockets for RESP.
- Streamlit is the only optional external dependency.
- Do not use `redis-py`.
- Do not use an arbitrary SQL editor.
- Handle errors gracefully and show them in the UI instead of raising an unhandled traceback.
- Keep the UI read-only.

## RESP Helpers

The demo can reuse the same basic raw RESP concepts as the benchmark scripts:

- `encode_command(*parts)`
- `read_response(sock)`
- support simple strings, errors, bulk strings, null bulk strings, and arrays.

For each Redis action, the client can open a short-lived TCP connection, send `SELECT 0`, verify `OK`, then send `GET` or `MGET`. This is fine for a demo UI because it is not a benchmark replacement.

## Key Formatting

For `String` keys, SQL should quote the key as a string literal.

For `UInt64` keys, SQL should render numeric keys without quotes after strict local validation, for example:

```sql
WHERE key = 0
```

For the Redis endpoint, keys are sent as RESP bulk strings in both modes. The ClickHouse endpoint decides whether to parse them as `String` or `UInt64` based on the configured table primary-key type.

## Prerequisites

- ClickHouse server is running.
- `redis_port=9006` is enabled.
- Redis DB 0 is mapped to the selected table.
- Example mapping:

```text
redis.db._0.database=bench
redis.db._0.table=kv_test
redis.db._0.default_column=value
```

For the default demo inputs, the table should be:

```text
bench.kv_test
```

with:

- `key UInt64`
- `value String`
- engine `EmbeddedRocksDB`

## Out Of Scope

- Not a benchmark replacement.
- Not a production UI.
- No writes.
- No arbitrary SQL editor.
- No authentication workflow.
- No config editing.
- No automatic server start/stop.

## Suggested README Content

`demo/README.md` should include:

- Required server prerequisites.
- Streamlit install command if missing.
- How to launch:

```bash
streamlit run demo/redis_endpoint_demo.py
```

- Default table/mapping assumptions.
- A note that latency is displayed for illustration only and should not be treated as benchmark data.
