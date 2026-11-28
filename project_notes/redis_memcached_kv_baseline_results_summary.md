# Baseline Benchmark Results Summary

This file summarizes preliminary baseline results before implementing the Redis/Memcached-compatible endpoint.

## Benchmark Date

2026-05-06

## Machine Configuration

- CPU: AMD EPYC 9555P 64-Core Processor, 8 vCPUs visible, 1 thread per core, 1 socket.
- RAM: 31 GiB total, 27 GiB available at measurement time.
- Disk: `/dev/sda1`, 180 GiB total, 87 GiB available, mounted on `/`.
- OS: Linux `scw-vigilant-visvesvaraya` 6.8.0-106-generic x86_64.
- ClickHouse commit: `ab49e6bf698`.
- ClickHouse binary: `./build-new/programs/clickhouse`.
- ClickHouse version/build id: `26.5.1.1`, build id `AF7A89914D34853DE648C56F3FD3344643D7A349`.
- Redis version: Redis server `7.0.15`, `jemalloc-5.3.0`.
- Build type: local ClickHouse build binary; exact build type was not recorded in this baseline.

## Dataset Description

- Number of keys: 100000
- Key format: `key_000000000`
- Value size: 64 bytes
- Distribution of requested keys: sequential keys from generated keys file `/tmp/kv_baseline_100k/keys_100k.txt`
- Table engine: `EmbeddedRocksDB`

## ClickHouse Table Schema

```sql
CREATE TABLE kv_baseline
(
    key String,
    value String
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key;
```

## Result Table

| system | interface | dataset_size | value_size | batch_size | concurrency | qps | p50_ms | p95_ms | p99_ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| clickhouse | http | 100000 | 64 | 1 | 1 | 1126.01 | 0.875 | 0.991 | 1.043 |
| redis | resp | 100000 | 64 | 1 | 1 | 48602.15 | 0.019 | 0.026 | 0.028 |
| clickhouse | http | 100000 | 64 | 10 | 8 | 1768.81 | 4.218 | 7.256 | 9.263 |
| redis | resp | 100000 | 64 | 10 | 8 | 19595.82 | 0.220 | 1.204 | 2.377 |
| clickhouse | http | 100000 | 64 | 100 | 32 | 1369.48 | 21.235 | 38.662 | 49.447 |
| redis | resp | 100000 | 64 | 100 | 32 | 6335.29 | 3.882 | 13.347 | 19.882 |

## Preliminary Interpretation

- What the numbers show: in this preliminary 100k baseline, ClickHouse HTTP SQL point lookup is functional and stable enough for a first baseline, but request QPS is much lower than the Redis reference. The gap is visible both for single-key requests and for batched requests.
- Whether SQL point lookup overhead is visible: the single-key ClickHouse HTTP SQL result shows p99 around 1.0 ms at concurrency 1, while Redis `GET` is around 0.03 ms in this local run. This is consistent with measurable overhead from the SQL/query-processing and HTTP result path, but it is not yet a final attribution.
- How Redis reference compares: Redis is a specialized in-memory key-value server and should be treated as a reference baseline, not as a requirement for ClickHouse to match exactly. It shows the approximate scale of a lightweight key-value protocol path on the same machine.
- What should be checked in the final before/after benchmark: after implementing the Redis-compatible ClickHouse endpoint, the same dataset, value size, batch sizes, and concurrency levels should be repeated. The important comparison is ClickHouse SQL HTTP baseline versus the new ClickHouse key-value endpoint, with Redis kept as an external reference.

## How These Results Motivate the Implementation

The baseline provides numerical motivation for a specialized read-only Redis-compatible endpoint over `IKeyValueEntity`. For single-key requests at concurrency 1, ClickHouse HTTP SQL reached about 1126 QPS with p99 around 1.0 ms, while the Redis reference reached about 48602 QPS with p99 around 0.03 ms. For `batch_size=10`, `concurrency=8`, ClickHouse HTTP SQL reached about 1769 requests/s with p99 around 9.3 ms; Redis `MGET` reached about 19596 requests/s with p99 around 2.4 ms. For `batch_size=100`, `concurrency=32`, ClickHouse HTTP SQL reached about 1369 requests/s with p99 around 49.4 ms; Redis `MGET` reached about 6335 requests/s with p99 around 19.9 ms.

These preliminary numbers justify testing whether a ClickHouse endpoint that bypasses SQL parsing, query analysis, planning, pipeline setup, and general-purpose result serialization can reduce overhead for prepared read-only key-value datasets. The final conclusion should come only after comparing this baseline with the implemented ClickHouse Redis-compatible endpoint, especially on p95/p99 latency and batched lookup behavior.

## Notes

Raw generated datasets and large CSV outputs should not be committed. Keep only concise summaries, scripts if needed later, and small representative result tables.
