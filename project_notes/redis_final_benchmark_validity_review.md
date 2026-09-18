# Final Benchmark Validity Review

## Summary

The final 100k benchmark matrix in `benchmark/kv_baseline/results/final_100k_raw.csv` is internally consistent at the CSV level. It contains 27 rows: 3 interfaces, 3 batch sizes, and 3 concurrency levels, with exactly one row for each expected combination.

No clear script bug was found that requires changing the benchmark scripts before interpreting the current results. The results are plausible, but they should be presented carefully. The strongest supported claim is that the ClickHouse Redis-compatible endpoint reduces overhead substantially compared with the current ClickHouse HTTP SQL benchmark. Redis should remain an external reference, not a required target and not a basis for claiming that ClickHouse is faster than Redis.

## Matrix Completeness

Expected combinations are present:

- Interfaces:
  - `clickhouse,http`
  - `clickhouse,redis_endpoint`
  - `redis,resp_raw`
- Batch sizes: 1, 10, 100
- Concurrency levels: 1, 8, 32
- Total rows: 27

One ClickHouse Redis-compatible row, batch-size 1 and concurrency 8, was rerun after an initial connection reset. The final CSV contains one successful row for that combination.

## Comparable Units

All three scripts report QPS as completed benchmark commands per second:

- `bench_clickhouse_http.py`: one HTTP SQL request is one counted request.
- `bench_clickhouse_redis.py`: one `GET` or one `MGET` command is one counted request.
- `bench_redis_raw.py`: one `GET` or one `MGET` command is one counted request.

For batch-size 100, QPS is command/request QPS, not individual-key QPS. Percentiles are measured per command/request, not per key. This is methodologically consistent across the three scripts, but the README and result summary should state it explicitly because `MGET` rows carry more keys per measured request.

## Script Review

### ClickHouse HTTP SQL

`bench_clickhouse_http.py` builds one SQL query per measured request. For batch-size 1 it uses:

```sql
WHERE key = '...'
```

For batch-size greater than 1 it uses:

```sql
WHERE key IN (...)
```

The script sends the query through `urllib.request.urlopen` with a new `Request` object per measured request. There is no explicit connection pool or persistent per-worker HTTP connection. This likely means the HTTP result includes more client/protocol overhead than a tuned HTTP client with connection reuse would have. This is not a script bug, because it still measures the script as written, but it is a major fairness caveat when comparing HTTP SQL against the raw TCP Redis-compatible endpoint.

The HTTP script reads the full response body but does not validate row count or values. If the query returned zero rows without an HTTP error, it would still count as success. Given the dataset checks below, this probably did not invalidate the current run, but stronger result validation would make future runs safer.

### ClickHouse Redis-Compatible Endpoint

`bench_clickhouse_redis.py` is structurally sound for this benchmark:

- Each worker opens one TCP connection.
- Each worker sends `SELECT 0` once and verifies `OK`.
- Batch-size 1 uses `GET`.
- Batch-size greater than 1 uses `MGET`.
- Each measured command sends one RESP command and reads exactly one RESP response.
- `GET` accepts bulk string or null bulk string.
- `MGET` requires an array and validates each item as bulk string or null bulk string.
- Redis error responses are counted as errors and fail the run before writing CSV.

It does not accidentally measure only client-side loop overhead because every measured iteration performs `sendall`, reads a full RESP response, and validates the response shape before recording latency.

The validation does not require non-null values or compare returned bytes against an expected value. That is acceptable for a fast benchmark, but it is a threat to validity if the data load is wrong or if a server silently returns misses.

### Redis Raw Reference

`bench_redis_raw.py` uses the same raw-socket RESP style as `bench_clickhouse_redis.py`:

- Python standard library only.
- One TCP connection per worker.
- One `SELECT 0` per worker.
- `GET` for batch-size 1.
- `MGET` for batch-size greater than 1.
- One response read per measured request.
- Same response-shape validation approach.

This makes the ClickHouse Redis-compatible endpoint and Redis reference much more comparable than the first sanity run that used a different Redis client.

## Dataset Fairness

Observed checks:

- Key file: `/tmp/kv_baseline_100k/keys_100k.txt`
- Key file rows: 100000
- Unique keys: 100000
- First key: `key_000000000`
- Last key: `key_000099999`
- ClickHouse `kv_baseline` row count: 100000
- Redis `DBSIZE`: 100000
- Sample key `key_000000001` returned the same 64-byte value from ClickHouse and Redis.

The scripts use the same deterministic request order and key distribution. Worker `N` starts at offset `N * batch_size`, and each worker advances by `batch_size * concurrency`. This is consistent across the three scripts.

The current evidence supports that the same 100k generated dataset was loaded into both systems. A stronger future check would compare more random sample values across ClickHouse and Redis before the run.

## Server And Process Fairness

Observed process/listener checks:

- One ClickHouse server process was visible:
  - `./build-new/programs/clickhouse server --config-file=/root/work/benchmarks/ch-runtime/config.xml`
- One Redis server process was visible:
  - `/usr/bin/redis-server 127.0.0.1:6379`
- ClickHouse owned local listeners including:
  - `127.0.0.1:8123`
  - `127.0.0.1:9006`
- Redis owned:
  - `127.0.0.1:6379`
  - `[::1]:6379`

The benchmark commands used `127.0.0.1`, so they should have hit the IPv4 listeners above. There was no evidence of a stale competing ClickHouse or Redis process on the benchmark ports.

## Result Plausibility

The Redis-compatible endpoint being close to Redis raw is plausible under these conditions:

- The workload is tiny and hot: 100000 keys with 64-byte values.
- Both Redis-style benchmarks use local loopback TCP and raw RESP clients.
- `GET` and `MGET` avoid SQL parsing and HTTP request handling.
- The ClickHouse implementation uses the key-value access path through `IKeyValueEntity`.
- `EmbeddedRocksDB` data may be heavily cached by RocksDB block cache and/or the OS page cache.
- The Python client and loopback networking can become a meaningful part of measured latency at these small values.

Redis raw being only modestly ahead does not prove that ClickHouse matches Redis generally. It means that on this specific hot, local, read-only, small-value, command-QPS benchmark, the ClickHouse Redis-compatible endpoint is in the same rough range.

The HTTP SQL numbers are also plausible, but they likely include overhead from SQL parsing, HTTP request processing, and probably per-request HTTP connection setup. That makes the HTTP baseline useful as a practical comparison for this script, but not the fastest possible ClickHouse-over-HTTP baseline.

## Suspicious Or Important Findings

- The HTTP benchmark likely does not reuse connections. This can exaggerate the gap between HTTP SQL and raw TCP RESP.
- The scripts report request QPS, not key QPS. For `MGET`, batch-size 100 means each request contains 100 keys.
- Response validation checks shape, not exact content. Missing keys would still be accepted by the Redis-style benchmarks because null bulk strings are valid RESP values.
- The HTTP benchmark does not validate row count or value content.
- Only one run per combination was recorded. There are no repetitions, medians, confidence intervals, or variance estimates.
- The benchmark is a hot-cache local-loopback microbenchmark. It should not be generalized to larger datasets, cold cache, remote clients, mixed operations, writes, misses, or multi-node scenarios.
- The raw Redis reference has lower p99 at the highlighted points and better best QPS, so any wording that implies ClickHouse is faster than Redis would be overclaiming.

## Actual Script Bugs

No definite script bug was found in the inspected benchmark logic.

Potential improvements before future publication are not urgent bug fixes:

- Add explicit documentation that QPS and percentiles are per request/command.
- Add optional strict validation that `GET` returns non-null values and that `MGET` returns the expected number of non-null values.
- Add row-count or value validation for HTTP SQL responses.
- Consider a persistent-connection HTTP benchmark variant if the report needs a tuned HTTP baseline.

## What Should Change Before Using Results In The Report

Before using these results in the thesis/report, update the methodology text to say:

- QPS is command/request QPS.
- Latency percentiles are per command/request.
- For `MGET`, per-key throughput can be estimated separately as command QPS multiplied by batch size, but latency is still per `MGET` command.
- The HTTP SQL benchmark uses Python `urllib.request` and does not explicitly reuse a persistent connection.
- Redis is included as an external reference, not as a required target.
- These are preliminary 100k hot-cache local-loopback results.

If time allows, add a small derived table with per-key throughput for batch-size 10 and 100. Keep the original command-QPS table as the primary measured result.

## Recommended Report Wording

Recommended wording:

> On the 100k-key local benchmark, the Redis-compatible endpoint substantially reduces protocol and query-processing overhead compared with the tested ClickHouse HTTP SQL path. QPS is measured per request: one `GET`, one `MGET`, or one HTTP SQL query. For `MGET`, larger batch sizes therefore represent more keys per request, not directly comparable per-key latency.

> The Redis raw-socket benchmark is included only as an external reference. It uses the same raw RESP client style as the ClickHouse Redis-compatible endpoint benchmark, which makes the comparison more useful than the earlier sanity run. However, Redis is not a mandatory target, and these local hot-cache results should not be generalized to broader Redis workloads.

Avoid wording like:

- "ClickHouse is faster than Redis."
- "The Redis-compatible endpoint matches Redis performance."
- "The endpoint eliminates all SQL overhead."
- "The result is representative of production deployments."

Safer wording:

- "The Redis-compatible endpoint is in the same rough range as the Redis raw reference on this specific local 100k read-only benchmark."
- "The main measured improvement is over the ClickHouse HTTP SQL baseline."
- "The benchmark suggests that protocol and SQL/HTTP overhead dominate the HTTP SQL baseline for small point lookups."

## Optional Follow-Up Checks

- Repeat the full 100k matrix several times and report medians.
- Add a 1M-key dataset run if time allows.
- Record whether runs are warm-cache or cold-cache; current results should be treated as warm/hot-cache.
- Add a persistent-connection HTTP benchmark variant. Raw socket HTTP is not directly comparable to RESP, but persistent HTTP would separate SQL overhead from connection setup overhead.
- Add per-key throughput calculations for `MGET` rows.
- Add strict response validation for sampled or all returned values.
- Add explicit server settings and cache-state notes to the final report.
