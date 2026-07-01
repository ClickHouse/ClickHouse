# Redis-Compatible Endpoint Final Benchmark Results

## Research Question Framing

The benchmark evaluates whether a Redis-compatible read-only endpoint can reduce ClickHouse SQL/HTTP overhead for supported point lookups over prepared key-value tables.

The Redis-compatible endpoint removes much of the SQL/HTTP overhead for supported point lookups over prepared key-value tables. The primary comparison is ClickHouse HTTP SQL path versus the ClickHouse Redis-compatible endpoint. Redis raw is included as an external reference baseline, not as a target to outperform.

## Run Metadata

- Date: 2026-05-09
- Short SHA: `f3509cd3fdd`
- Raw input CSV: `benchmark/kv_baseline/results/final_100k_raw.csv`

## Machine Configuration

- CPU: AMD EPYC 9555P 64-Core Processor, 8 vCPUs visible, 1 thread per core
- RAM: 31 GiB
- OS: Ubuntu 24.04.4 LTS
- ClickHouse version: 26.5.1.1
- Redis version: 7.0.15, jemalloc 5.3.0
- ClickHouse binary path: `./build-new/programs/clickhouse`

## 100k String-Key Benchmark Dataset

- Keys: 100000
- Key type: `String`
- Key format: `key_000000000`
- Value type: `String`
- Value size: 64 bytes
- ClickHouse table engine: `EmbeddedRocksDB`
- The same generated dataset was used for all systems.

## 100k String-Key Interfaces Compared

- ClickHouse HTTP SQL
- ClickHouse Redis-compatible endpoint measured with a raw socket RESP client
- Redis reference measured with a raw socket RESP client

## Methodology Note

The first sanity run compared the ClickHouse Redis-compatible endpoint through a raw socket client against Redis through a different client. This final summary uses `bench_redis_raw.py`, so both the ClickHouse Redis-compatible endpoint and the Redis reference use the same raw-socket RESP client style.

Redis remains an external reference for orientation. It is not a required performance target for the ClickHouse endpoint.

## Representative Results

| System | Interface | Batch size | Concurrency | QPS | p50 ms | p95 ms | p99 ms |
|---|---|---:|---:|---:|---:|---:|---:|
| ClickHouse | HTTP SQL | 1 | 1 | 1143.15 | 0.861 | 0.974 | 1.021 |
| ClickHouse | HTTP SQL | 1 | 8 | 2763.83 | 2.676 | 4.568 | 6.006 |
| ClickHouse | HTTP SQL | 1 | 32 | 2827.14 | 10.349 | 18.095 | 22.646 |
| ClickHouse | HTTP SQL | 10 | 8 | 1822.13 | 4.135 | 6.755 | 8.359 |
| ClickHouse | HTTP SQL | 100 | 32 | 1579.83 | 17.927 | 36.167 | 50.651 |
| ClickHouse | Redis endpoint | 1 | 1 | 62842.53 | 0.014 | 0.018 | 0.021 |
| ClickHouse | Redis endpoint | 1 | 8 | 71010.11 | 0.084 | 0.283 | 0.416 |
| ClickHouse | Redis endpoint | 1 | 32 | 84491.87 | 0.287 | 1.006 | 1.457 |
| ClickHouse | Redis endpoint | 10 | 8 | 38504.39 | 0.157 | 0.507 | 0.741 |
| ClickHouse | Redis endpoint | 100 | 32 | 7811.95 | 3.202 | 9.940 | 14.222 |
| Redis | RESP raw | 1 | 1 | 83097.33 | 0.011 | 0.013 | 0.015 |
| Redis | RESP raw | 1 | 8 | 59741.45 | 0.100 | 0.347 | 0.524 |
| Redis | RESP raw | 1 | 32 | 90535.71 | 0.267 | 0.939 | 1.354 |
| Redis | RESP raw | 10 | 8 | 40095.66 | 0.152 | 0.482 | 0.713 |
| Redis | RESP raw | 100 | 32 | 8774.18 | 2.838 | 8.836 | 12.480 |

## 100k String-Key Key Results

- Best QPS:
  - HTTP SQL: 2827.14
  - ClickHouse Redis endpoint: 84491.87
  - Redis raw reference: 90535.71
- p99 at batch-size 1, concurrency 1:
  - HTTP SQL: 1.021 ms
  - ClickHouse Redis endpoint: 0.021 ms
  - Redis raw reference: 0.015 ms
- p99 at batch-size 100, concurrency 32:
  - HTTP SQL: 50.651 ms
  - ClickHouse Redis endpoint: 14.222 ms
  - Redis raw reference: 12.480 ms

## Additional Existing-Table Benchmark: `bench.kv_test`

This benchmark verifies the Redis-compatible endpoint on a larger existing ClickHouse table with `UInt64` keys, not only on the generated 100k `String` key benchmark table.

Dataset and schema:

- Table: `bench.kv_test`
- Rows: 10M
- Key: `UInt64`
- Value: `String`
- Extra columns: `extra1 UInt32`, `extra2 Float64`
- Engine: `EmbeddedRocksDB`
- Primary key: `key`

Results:

| Metric | ClickHouse HTTP SQL | ClickHouse Redis endpoint |
|---|---:|---:|
| Best QPS | 2763.65 | 80652.74 |
| p99, batch-size 1, concurrency 1 | 1.059 ms | 0.028 ms |
| p99, batch-size 100, concurrency 32 | 52.755 ms | 19.824 ms |

The `bench.kv_test` result supports the same interpretation as the 100k benchmark: the Redis-compatible endpoint substantially reduces overhead relative to the tested ClickHouse HTTP SQL path for prepared key-value point lookups. It also verifies the `UInt64` key path used by `IKeyValueEntity::getByKeys`.

## QPS And `MGET` Interpretation

QPS is measured per command/request:

- `GET` is one command.
- `MGET` is one command containing multiple keys.
- HTTP SQL batch lookup is one HTTP SQL request.

For `MGET`, approximate key throughput is request QPS multiplied by batch size. Latency percentiles remain per request/command, not per key.

## Interpretation

The ClickHouse Redis-compatible endpoint substantially reduces overhead compared with ClickHouse HTTP SQL on this 100k-key point lookup benchmark. At batch-size 1 and concurrency 1, QPS increases from 1143.15 to 62842.53, which is about 55x higher. At the best observed point for each interface, QPS increases from 2827.14 for HTTP SQL to 84491.87 for the Redis endpoint, which is about 30x higher.

Latency improves at all representative points. For batch-size 1 and concurrency 1, p50 improves from 0.861 ms to 0.014 ms, p95 from 0.974 ms to 0.018 ms, and p99 from 1.021 ms to 0.021 ms. At batch-size 100 and concurrency 32, p99 improves from 50.651 ms to 14.222 ms.

Batching affects the systems differently. HTTP SQL does not benefit in this setup, because larger batches still pay SQL/HTTP overhead and the measured request rate drops as batch size grows. The Redis-compatible endpoint remains much faster than HTTP SQL, but larger `MGET` batches reduce command QPS because each command carries more keys and response payload. The batch-size 100, concurrency 32 row should be read as command throughput and latency per command, not per-key throughput.

Compared with the Redis raw reference, the ClickHouse Redis-compatible endpoint is close but should not be overclaimed as identical. Redis raw has the best observed QPS, 90535.71 versus 84491.87 for the ClickHouse Redis endpoint. Redis raw also has slightly lower p99 at the two highlighted points: 0.015 ms versus 0.021 ms at batch-size 1, concurrency 1, and 12.480 ms versus 14.222 ms at batch-size 100, concurrency 32. The comparison is now fairer than the first sanity run because both Redis-style rows use raw socket RESP clients.

## Research Question

The Redis-compatible endpoint does reduce overhead compared with SQL/HTTP. The strongest result is for small point lookups, especially batch-size 1 across the tested concurrency levels. It also remains better than HTTP SQL for larger batches and higher concurrency, including batch-size 100 at concurrency 32.

The effect is clearest when the workload is dominated by point lookup protocol overhead. Under batch-size 1, the endpoint avoids SQL parsing and HTTP request overhead for each lookup. Under higher concurrency, the Redis-compatible endpoint continues to scale to much higher command QPS than HTTP SQL, while keeping much lower p50, p95, and p99 latency in the representative rows.

The experiments support the claim that a specialized Redis-compatible read-only endpoint can substantially reduce SQL/HTTP overhead for supported point lookups over prepared ClickHouse key-value tables. This is shown both on the 100k `String` key benchmark and on the 10M-row `UInt64` `bench.kv_test` table. The result does not prove production-wide performance and does not imply that ClickHouse is generally faster than Redis.

## Limitations And Caveats

- These results are hot-cache local-loopback microbenchmarks.
- The results do not prove production-wide performance.
- QPS is command/request QPS, not per-key QPS.
- For `MGET`, approximate key throughput is request QPS multiplied by batch size, while latency remains per request/command.
- The Redis-compatible endpoint is not a full Redis implementation.
- The Redis-compatible endpoint is not a Redis replacement.
- Redis raw is an external reference baseline, not something the ClickHouse endpoint is claimed to outperform generally.
- Do not claim that ClickHouse is faster than Redis.
- The endpoint supports prepared `IKeyValueEntity`-compatible key-value tables, not arbitrary ClickHouse tables.
- Current scope is read-only `GET`/`MGET`, `String` or `UInt64` key, and `String` `default_column` value.
- The comparison does not cover cold cache, remote clients, write workloads, mixed workloads, persistence effects, failures, or multi-node deployment conditions.

## Notes

- The raw CSV `benchmark/kv_baseline/results/final_100k_raw.csv` should not be committed.
- The raw CSV files under `benchmark/kv_baseline/results/` should not be committed unless explicitly intended as small curated artifacts.
