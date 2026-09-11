# Redis Demo Current State

## Current Demo Structure

The current demo is implemented in `demo/redis_endpoint_demo.py` as a small Streamlit app titled `ClickHouse Key-Value Access Demo`.

Existing Streamlit structure:

- Sidebar configuration.
- Main text describing the comparison between the normal SQL/HTTP path and the Redis-compatible `GET`/`MGET` endpoint.
- Two tabs:
  - `GET`
  - `MGET`

Existing inputs:

- ClickHouse HTTP host, default `127.0.0.1`.
- ClickHouse HTTP port, default `8123`.
- Redis endpoint host, default `127.0.0.1`.
- Redis endpoint port, default `9006`.
- Database, default `bench`.
- Table, default `kv_test`.
- Key column, default `key`.
- Value column, default `value`.
- Key type selector: `String` or `UInt64`, default `UInt64`.
- Single key, default `0`.
- Comma-separated `MGET` keys, default `0,65536,131072`.

Existing SQL execution logic:

- Builds SQL locally from sidebar inputs.
- `GET` SQL shape:

```sql
SELECT <value_column> FROM <database>.<table> WHERE <key_column> = <key> LIMIT 1
```

- `MGET` SQL shape:

```sql
SELECT <key_column>, <value_column>
FROM <database>.<table>
WHERE <key_column> IN (...)
FORMAT TSV
```

- Uses `http.client.HTTPConnection`.
- Sends a `POST /` request with the SQL query as the request body.
- Reads the full HTTP response body.
- Converts HTTP status `>= 400` into a user-visible error.
- Catches connection and request exceptions and returns them as `QueryResult.error`.

Existing Redis execution logic:

- Uses Python standard-library raw sockets.
- Encodes commands as RESP arrays with bulk-string arguments.
- Opens a TCP connection to the configured Redis-compatible endpoint.
- Sends commands sequentially on one connection.
- For `GET`, sends:

```text
SELECT 0
GET <key>
```

- For `MGET`, sends:

```text
SELECT 0
MGET <key1> <key2> ...
```

- Parses RESP simple strings, errors, bulk strings, null bulk strings, and arrays.
- Does not use `redis-py` or another Redis client library.

Existing latency measurement:

- SQL and Redis paths both use `time.perf_counter`.
- Latency is measured around the full client-side request execution.
- Redis latency includes TCP connect, `SELECT`, and the target `GET` or `MGET` command because a new connection is opened for each UI action.
- These latency values are suitable as illustrative demo values, not benchmark results.

Existing result comparison:

- `GET` parses the first non-empty SQL response line and compares it with the parsed Redis value.
- `MGET` parses SQL `FORMAT TSV` rows into a key-to-value dictionary.
- `MGET` compares Redis array elements against SQL rows in the original requested key order.
- This correctly handles the fact that SQL `IN` does not guarantee output ordering.

Existing benchmark summary:

- The current Streamlit app does not show benchmark results.
- `demo/README.md` explicitly says latency values are illustrative and should not be used as benchmark data.

## Current Dependencies

- Streamlit is the only UI dependency.
- Python standard library:
  - `http.client`
  - `socket`
  - `time`
  - `dataclasses`
  - `typing`
- HTTP access uses Python standard-library `http.client`.
- Redis-compatible access uses Python standard-library raw sockets.
- No Redis Python library is used.

## Already Satisfies The New Demo / Proof Goal

- The demo already compares the same logical lookup through SQL/HTTP and Redis-compatible paths.
- The demo already displays generated SQL for `GET` and `MGET`.
- The demo already displays generated Redis command text for `GET` and `MGET`.
- The demo already shows returned values and user-visible errors.
- The demo already shows illustrative latency for both paths.
- The demo already supports both `String` and `UInt64` key formatting for SQL.
- The demo already preserves requested key order for `MGET` comparison.
- The demo already avoids Redis client dependencies by using raw RESP over sockets.
- The README already frames the app as a local defense demo, not production code or a benchmark replacement.

## Missing

- There is no `Overview` tab showing active endpoint addresses, selected table, key type, Redis DB index, read-only scope, and supported commands.
- There is no combined `Compare GET/MGET` tab with controls to run SQL only, Redis only, or both.
- The SQL request is displayed, but there is no explicit raw HTTP request display.
- The Redis commands are displayed, but there is no raw RESP request display.
- There is no request path diagram.
- There is no explicit statement in the UI that the Redis-compatible path avoids SQL parsing, planning, and query pipeline construction for supported `GET`/`MGET` requests.
- There is no `Manual Console` tab with editable SQL and Redis command textareas.
- There is no `Proof / Live Verification` tab.
- There is no Streamlit button to check recent SQL entries in `system.query_log`.
- There are no copyable terminal commands in the UI for:
  - `ss -ltnp | grep 9006`
  - `pgrep -a clickhouse`
  - `system.query_log` inspection
  - `RedisHandler` log tailing
- There is no `Benchmark Context` tab.
- Benchmark caveats are not shown in the app.
- The README does not yet describe a defense walkthrough scenario using Overview, Request Path, Manual Console, Proof / Live Verification, and Benchmark Context.

## Risks

- The demo should not crash if the ClickHouse HTTP endpoint is down. The current SQL helper catches exceptions and returns a user-visible error, which is good, but future manual-console code must keep this behavior.
- The demo should not crash if the Redis-compatible endpoint is down. The current Redis helper catches socket and RESP errors, which is good, but future UI paths must not bypass that wrapper.
- Result normalization may be incorrect for values containing trailing newlines, tabs, binary bytes, or non-UTF-8 payloads. The current implementation decodes Redis bulk strings as UTF-8 with replacement and strips trailing newlines for display.
- `GET` comparison currently treats an empty SQL result and a Redis null bulk string as equal. That is reasonable for missing keys, but it may hide unexpected empty SQL output if the SQL query is malformed or the table is wrong.
- SQL and Redis ordering may differ for `MGET` unless handled carefully. The current code handles this by converting SQL rows to a dictionary and comparing in requested key order; this behavior should be preserved.
- Duplicate keys in `MGET` may need attention. Redis returns one value per requested key, while SQL rows keyed into a dictionary collapse duplicates. For the demo, either document this or preserve duplicates by mapping each requested key to the same SQL value.
- `system.query_log` may not flush immediately. A live proof button should either call `SYSTEM FLUSH LOGS` before reading or explain the delay.
- Log paths are environment-specific. Any `tail` command should be presented as an example with configurable path.
- Redis `SELECT` DB index is currently hardcoded as `0` in the demo request execution. The proof UI plan requires showing Redis DB index, so implementation should make it configurable while keeping default `0`.

## Implementation Recommendation

- Use minimal changes focused on demo clarity, not architectural changes.
- Keep the demo read-only.
- Keep using raw sockets for Redis-compatible endpoint access.
- Avoid new dependencies beyond Streamlit and the Python standard library.
- Keep all connection and parsing errors user-friendly through `QueryResult.error` or an equivalent wrapper.
- Preserve the current `MGET` order-aware comparison.
- Add the new tabs incrementally around the existing helpers instead of rewriting the app.
- Add manual console support by reusing the existing SQL HTTP helper and RESP helper.
- Add query-log proof by issuing a normal SQL query over the configured HTTP endpoint; do not invoke external shell commands from the UI.
- Show terminal commands as copyable text, not as commands executed by Streamlit.
- Mark all UI latency numbers as illustrative and keep benchmark numbers static.
