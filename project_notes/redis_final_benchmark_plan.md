# Final Before/After Benchmark Plan

This plan defines the final benchmark for comparing the existing ClickHouse HTTP SQL baseline, the implemented ClickHouse Redis-compatible endpoint, and the Redis reference on the same key-value dataset.

## Goal

Compare:

- `clickhouse/http_sql`
- `clickhouse/redis_endpoint`
- `redis/resp`

All systems should use the same generated key-value dataset so the final result measures interface and request-path overhead rather than dataset differences.

## Main Research Question

How much point-lookup overhead can be reduced by bypassing the SQL/HTTP path and using a Redis-compatible endpoint over `IKeyValueEntity`?

The primary before/after comparison is ClickHouse HTTP SQL lookup versus the ClickHouse Redis-compatible endpoint. Redis is kept as an external reference for the cost profile of a specialized key-value protocol, not as a mandatory target.

## Dataset

- First final benchmark: 100k keys.
- Value size: 64 bytes.
- Reuse the same generated data as the baseline if it is still available under `/tmp/kv_baseline_100k`.
- Optional later benchmark: 1M keys, if time allows.

The benchmark should use the same key format and request-key order as the existing baseline where possible. This makes the comparison with `project_notes/redis_memcached_kv_baseline_results_summary.md` direct.

## Systems and Interfaces

| System | Interface | Benchmark utility |
|---|---|---|
| `clickhouse` | `http_sql` | `benchmark/kv_baseline/bench_clickhouse_http.py` |
| `clickhouse` | `redis_endpoint` | `benchmark/kv_baseline/bench_clickhouse_redis.py` |
| `redis` | `resp` | `benchmark/kv_baseline/bench_redis.py` |

The new `bench_clickhouse_redis.py` should follow the same output conventions as the existing baseline utilities so the result rows can be appended to one CSV file.

## Workloads

- `batch_size=1`
- `batch_size=10`
- `batch_size=100`

For the ClickHouse Redis-compatible endpoint:

- `batch_size=1` uses `GET`.
- `batch_size>1` uses `MGET`.

## Concurrency

- `1`
- `8`
- `32`

Each system/interface should be tested across every batch-size and concurrency combination when practical.

## Metrics

- QPS.
- p50 latency.
- p95 latency.
- p99 latency.

Tail latency is part of the final conclusion. The interpretation should discuss p99 behavior, not only QPS.

## Required ClickHouse Redis Endpoint Behavior

Each worker connection must send `SELECT 0` once before issuing lookup commands. The later `GET` or `MGET` commands must run on the same connection after that `SELECT`.

This matters because the Redis-compatible endpoint maps `SELECT` to the key-value entity context. Sending `SELECT 0` on a different connection would not validate the intended steady-state lookup path.

Command selection:

- `batch_size=1`: send `GET key`.
- `batch_size>1`: send `MGET key1 key2 ... keyN`.

## Expected CSV Schema

```csv
system,interface,dataset_size,value_size,batch_size,concurrency,qps,p50_ms,p95_ms,p99_ms
```

Expected row examples:

```csv
clickhouse,http_sql,100000,64,1,1,0,0,0,0
clickhouse,redis_endpoint,100000,64,1,1,0,0,0,0
redis,resp,100000,64,1,1,0,0,0,0
```

## Result Interpretation

The final write-up should compare `clickhouse/http_sql` against `clickhouse/redis_endpoint` first. That is the main before/after result for the project.

The `redis/resp` result should be treated as an external reference. It helps show the scale of a specialized Redis protocol path on the same machine, but the ClickHouse Redis-compatible endpoint does not need to match Redis exactly for the project to be useful.

The interpretation should cover:

- QPS change from `clickhouse/http_sql` to `clickhouse/redis_endpoint`.
- p50, p95, and especially p99 latency change.
- Whether batching with `MGET` reduces per-key overhead compared with repeated single-key `GET`.
- Whether higher concurrency improves throughput or mainly increases tail latency.
- Whether the observed results support the research question about bypassing SQL parsing, query analysis, pipeline setup, HTTP handling, and general result serialization.

## Out of Scope

- gRPC.
- Flamegraphs.
- Mandatory 10M-key dataset.
- Non-`String` values.
- `MGET` pipeline depth experiments.

