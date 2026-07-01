# Baseline Benchmark Plan for Redis and Memcached Key-Value Project

This plan defines the first numerical baseline before implementing a Redis/Memcached-compatible endpoint.

## Why a Numerical Baseline Is Needed

The project should not rely only on the assumption that SQL point lookups are slower than key-value protocol lookups. It should first measure current ClickHouse behavior with ordinary point lookups. These results provide numerical motivation for the project, show where the current baseline stands, and make the final before/after comparison meaningful.

## Main Hypothesis

Ordinary SQL point lookups in ClickHouse have overhead from SQL parsing, query analysis, planning, query pipeline setup, and result serialization.

## ClickHouse Backend

Use an `EmbeddedRocksDB` table:

```sql
CREATE TABLE kv_baseline
(
    `key` String,
    `value` String
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY `key`;
```

This backend is a good first target because it has key-value semantics inside ClickHouse and implements `IKeyValueEntity`.

## Dataset Design

- 100k keys for quick smoke validation.
- 1M keys for the main baseline.
- 10M keys only as an optional final-scale experiment if the machine has enough resources.
- Key format: `key_000000000`.
- Value size: 64 bytes.
- Optional second value size: 256 bytes.

Example logical rows:

```text
key_000000000,value64...
key_000000001,value64...
key_000000002,value64...
```

## Systems and Interfaces to Compare

- ClickHouse SQL point lookup over HTTP.
- ClickHouse SQL point lookup over native TCP if the client dependency is easy to set up.
- Redis `GET`/`MGET` as a reference key-value protocol baseline on the same machine.

The Redis baseline is not a requirement for ClickHouse to match Redis exactly. It is a reference for the cost profile of a specialized key-value protocol on the same hardware.

## Workloads

- Single-key lookup.
- Batch lookup with 10 keys.
- Batch lookup with 100 keys.
- Optional later workload with Zipf/hot-key distribution.

Example ClickHouse SQL forms:

```sql
SELECT value FROM kv_baseline WHERE key = 'key_000123456';
```

```sql
SELECT key, value
FROM kv_baseline
WHERE key IN ('key_000123456', 'key_000123457', 'key_000123458');
```

Example Redis forms:

```text
GET key_000123456
MGET key_000123456 key_000123457 key_000123458
```

## Concurrency Levels

- 1
- 8
- 32

These levels are enough for the first baseline to show single-client latency, moderate concurrency behavior, and a higher-concurrency stress point.

## Metrics

- QPS.
- p50 latency.
- p95 latency.
- p99 latency.
- Optional p99.9 latency.
- Approximate CPU usage if easy to collect.

Tail latency should be reported because real-time serving scenarios are more sensitive to p95/p99 than to average latency.

## Out of Scope for the First Baseline

- Mandatory 10M keys.
- gRPC.
- Flamegraphs.
- Multiple storage engines.
- Cluster or `Distributed` tables.
- Realistic public ad-tech dataset.

These can be added later, but they should not block the first numerical baseline.

## Expected CSV Schema

```csv
system,interface,dataset_size,value_size,batch_size,concurrency,qps,p50_ms,p95_ms,p99_ms
clickhouse,http,1000000,64,1,1,0,0,0,0
clickhouse,http,1000000,64,10,8,0,0,0,0
redis,resp,1000000,64,100,32,0,0,0,0
```

Column meanings:

- `system`: `clickhouse` or `redis`.
- `interface`: `http`, `native`, or `resp`.
- `dataset_size`: number of keys loaded.
- `value_size`: value size in bytes.
- `batch_size`: number of keys per request.
- `concurrency`: number of concurrent clients or workers.
- `qps`: completed requests per second.
- `p50_ms`, `p95_ms`, `p99_ms`: latency percentiles in milliseconds.

## Expected Markdown Table for the Report

| System | Interface | Dataset size | Value size | Batch size | Concurrency | QPS | p50, ms | p95, ms | p99, ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ClickHouse | HTTP SQL | 1M | 64 | 1 | 1 | TBD | TBD | TBD | TBD |
| ClickHouse | HTTP SQL | 1M | 64 | 10 | 8 | TBD | TBD | TBD | TBD |
| ClickHouse | Native SQL | 1M | 64 | 1 | 1 | TBD | TBD | TBD | TBD |
| Redis | RESP | 1M | 64 | 1 | 1 | TBD | TBD | TBD | TBD |
| Redis | RESP | 1M | 64 | 10 | 8 | TBD | TBD | TBD | TBD |
| Redis | RESP | 1M | 64 | 100 | 32 | TBD | TBD | TBD | TBD |

The report table can be shortened to the most representative rows after full results are collected.

## How These Results Will Be Used

- In the introduction as numerical motivation.
- In Chapter 2 as problem analysis.
- As baseline for final before/after comparison.
- To justify the Redis-compatible endpoint.

The final report should compare the baseline SQL path with the implemented Redis-compatible endpoint using the same dataset sizes, value sizes, batch sizes, and concurrency levels where practical.

## Optional Later Profiling

`perf` and flamegraph profiling can be added after the initial baseline and prototype are working. Profiling should be used to explain observed bottlenecks, but it is not required before the first baseline table exists.
