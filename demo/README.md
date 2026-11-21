# ClickHouse Redis-Compatible Endpoint Demo

This is a small local demo for the graduation defense. It is not production code.

The demo compares two read-only access paths to the same ClickHouse data:

- ClickHouse HTTP SQL query.
- ClickHouse Redis-compatible read-only endpoint using `SELECT <db>` plus `GET` or `MGET`.

It shows the generated requests, returned values, illustrative client-side latency, result matching, request-path diagrams, live `system.query_log` proof, and static benchmark context.

## Installation

The demo uses Streamlit for the UI:

```bash
python3 -m pip install --user --break-system-packages streamlit
```

No Redis Python client is required. The demo uses Python standard-library sockets for RESP.

## Run

```bash
streamlit run demo/redis_endpoint_demo.py
```

## Prerequisites

- ClickHouse server is running.
- ClickHouse HTTP port is reachable, usually `127.0.0.1:8123`.
- `redis_port=9006` is enabled and reachable.
- Redis DB 0 is mapped to the selected table.

Example Redis mapping:

```text
redis.db._0.database=bench
redis.db._0.table=kv_test
redis.db._0.default_column=value
```

## Example For `bench.kv_test`

Use these UI inputs:

- Database: `bench`
- Table: `kv_test`
- Key column: `key`
- Value column: `value`
- Key type: `UInt64`
- Redis DB index: `0`
- Compare keys: `0,65536,131072`

## Defense Walkthrough

1. Open `Overview` and check the configured HTTP and Redis-compatible endpoints.
2. Open `Compare GET/MGET`, run both paths for one key and then multiple keys, and show that the results match.
3. Open `Request Path` and explain that supported `GET`/`MGET` requests avoid SQL parsing, planning, and query pipeline construction.
4. Open `Manual Console` to show the exact SQL and Redis command sequence.
5. Open `Proof / Live Verification`, run the `system.query_log` check, and show that SQL requests appear as SQL queries while Redis `GET`/`MGET` requests are handled by `RedisHandler`.
6. Open `Benchmark Context` to discuss static benchmark numbers and caveats.

## Limitations

- This is a local defense demo, not a benchmark replacement.
- Latency values are illustrative and should not be used as benchmark data.
- The demo is read-only.
- The SQL console is intended for local read-only `SELECT` demonstrations.
- The current endpoint returns one configured `String` `default_column`.
- The Redis-compatible endpoint is not a general-purpose Redis-compatible server.
- Benchmark numbers shown in the UI are hot-cache local-loopback measurements.
