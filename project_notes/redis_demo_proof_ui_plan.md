# Redis Demo Proof UI Plan

## Goal

Build a Streamlit defense demo that makes it visually clear how the same key lookup is executed through two different paths:

- Regular ClickHouse HTTP SQL path.
- Redis-compatible endpoint path.

The demo should show that the Redis-compatible endpoint is a read-only specialized path for prepared key-value tables, not a general Redis-compatible server and not an arbitrary SQL interface.

## Why This Demo Matters

- It explains where the benchmark numbers come from.
- It proves that the comparison uses two different request paths.
- It helps the defense audience understand why the Redis-compatible endpoint removes much of SQL/HTTP overhead for supported point lookups over prepared key-value tables.
- It avoids presenting only raw numbers without architectural context.

## Required UI Tabs

- Overview
- Compare `GET`/`MGET`
- Request Path
- Manual Console
- Proof / Live Verification
- Benchmark Context

## Overview Tab

The overview tab should show the active demo configuration in a compact form:

- ClickHouse HTTP endpoint address.
- Redis-compatible endpoint address.
- Selected ClickHouse database.
- Selected ClickHouse table.
- Key column.
- Value column.
- Key type.
- Redis DB index.
- Read-only scope.
- Supported commands: `PING`, `QUIT`, `SELECT`, `GET`, `MGET`.

The tab should explicitly say that the endpoint is read-only and supports only the implemented command subset needed for `GET`/`MGET` access to prepared key-value tables.

## Compare `GET`/`MGET` Tab

This tab should be the primary live comparison view.

Required controls:

- Input for one key.
- Input for multiple keys.
- Button to run only the SQL path.
- Button to run only the Redis-compatible endpoint path.
- Button to run both paths.

For the SQL side, the UI should show:

- Actual SQL query.
- Raw result.
- Parsed value or values.
- Latency.
- Short request-path summary: `HTTP -> SQL parser -> analyzer/planner -> query pipeline -> storage`.

For the Redis-compatible endpoint side, the UI should show:

- Actual Redis command sequence, for example `SELECT 0` then `GET 0` or `MGET 0 65536 131072`.
- Raw RESP response when practical.
- Parsed value or values.
- Latency.
- Short request-path summary: `RESP -> RedisProtocol -> RedisHandler -> IKeyValueEntity::getByKeys`.

When both paths are run, the UI should show a result equality indicator. For `MGET`, comparison must preserve the original key order because SQL `IN` does not guarantee output ordering.

## Request Path Tab

This tab should show two diagrams.

SQL path:

```text
Client -> HTTP -> SQL parser -> analyzer/planner -> query pipeline -> storage -> result
```

Redis-compatible endpoint path:

```text
Client -> TCP :9006 / RESP -> RedisProtocol -> RedisHandler -> DatabaseCatalog -> IKeyValueEntity::getByKeys -> RESP response
```

The tab should explicitly say:

> The Redis-compatible path avoids SQL parsing, planning, and query pipeline construction for supported `GET`/`MGET` requests.

It should also state that the Redis-compatible path still reads ClickHouse-managed data through the existing `IKeyValueEntity` abstraction.

## Manual Console Tab

This tab should allow controlled manual execution for defense demonstrations.

Required controls:

- SQL textarea.
- Redis commands textarea.
- Run SQL button.
- Run Redis commands button.
- Raw response area.
- Parsed response area, if parsing is available.
- Latency display.

Default example SQL:

```sql
SELECT value FROM bench.kv_test WHERE key = 0 FORMAT TabSeparated
```

Default example Redis commands:

```text
SELECT 0
GET 0
```

Default example Redis `MGET` commands:

```text
SELECT 0
MGET 0 65536 131072
```

The SQL console is for demonstration only. The Redis console should support one command per line and send the commands sequentially on one TCP connection so that `SELECT` state is preserved.

## Proof / Live Verification Tab

This tab should provide copyable terminal commands and one small live check from Streamlit.

Copyable terminal commands:

```bash
ss -ltnp | grep 9006
```

```bash
pgrep -a clickhouse
```

Example `system.query_log` query for SQL path evidence:

```sql
SELECT
    event_time,
    query_duration_ms,
    query
FROM system.query_log
WHERE event_time >= now() - INTERVAL 5 MINUTE
  AND type = 'QueryFinish'
  AND query ILIKE '%bench.kv_test%'
ORDER BY event_time DESC
LIMIT 10
```

Example log tail command for `RedisHandler` evidence:

```bash
tail -f ./store/clickhouse-server.log | grep RedisHandler
```

The exact log path may need to be configurable because local ClickHouse runs can use different log directories.

Required Streamlit button:

- Check recent SQL `query_log`.

The tab should explain:

- SQL path requests should appear in `system.query_log`.
- Redis `GET`/`MGET` requests are handled by `RedisHandler` and should not appear as SQL queries in `system.query_log`.
- `RedisHandler` logs can be tailed in a separate terminal if trace/debug logging exists.

This tab is not a security or audit system. It is a defense-oriented live proof that the two UI actions use different request paths.

## Benchmark Context Tab

This tab should show static benchmark context, not run benchmarks.

Include the available 100k String-key benchmark:

- HTTP SQL best QPS: about `2827`.
- ClickHouse Redis-compatible endpoint best QPS: about `84491`.
- Redis raw reference best QPS: about `90535`.
- p99 at batch-size `1`, concurrency `1`:
  - HTTP SQL: about `1.021 ms`.
  - ClickHouse Redis-compatible endpoint: about `0.021 ms`.
  - Redis raw reference: about `0.015 ms`.

Include the `bench.kv_test` 10M-row `UInt64` benchmark:

- Table: `bench.kv_test`.
- Rows: `10M`.
- Key: `UInt64`.
- Value: `String`.
- Extra columns: `extra1 UInt32`, `extra2 Float64`.
- Engine: `EmbeddedRocksDB`.
- HTTP SQL best QPS: `2763.65`.
- Redis endpoint best QPS: `80652.74`.
- p99 at batch-size `1`, concurrency `1`:
  - HTTP SQL: `1.059 ms`.
  - Redis endpoint: `0.028 ms`.
- p99 at batch-size `100`, concurrency `32`:
  - HTTP SQL: `52.755 ms`.
  - Redis endpoint: `19.824 ms`.

Required caveats:

- These are hot-cache local-loopback microbenchmarks.
- QPS is per command/request, not per key.
- For `MGET`, approximate key throughput can be calculated as request QPS multiplied by batch size, but latency remains per `MGET` command.
- The Redis-compatible endpoint is not a general Redis-compatible server.
- Redis is used as an external reference baseline, not something we claim to outperform generally.
- These numbers are not evidence for deployment-wide behavior.

## Expected Changed Files

The implementation stage for this UI is expected to change:

- `demo/redis_endpoint_demo.py`
- `demo/README.md`
- `project_notes/redis_demo_proof_ui_plan.md`

This planning stage changes only:

- `project_notes/redis_demo_proof_ui_plan.md`

## Wording Rules

Do not say:

- Claims that ClickHouse generally outperforms Redis.
- Claims that this implements broad Redis command compatibility.
- Claims that local demo results establish deployment-wide behavior.

Do say:

- "Redis-compatible endpoint removes much of SQL/HTTP overhead for supported point lookups over prepared key-value tables."
- "The endpoint is read-only and supports `GET`/`MGET`."
- "Redis is used as an external reference baseline."

## Implementation Notes For The Next Stage

- Keep the demo local and defense-focused.
- Do not run benchmarks from the UI.
- Make command text and generated queries copyable.
- Keep latency labels marked as illustrative UI measurements, not benchmark results.
- Keep the Redis path tied to configured `SELECT` mapping, not arbitrary table access.
- Prefer simple, explicit UI sections over decorative layout.
