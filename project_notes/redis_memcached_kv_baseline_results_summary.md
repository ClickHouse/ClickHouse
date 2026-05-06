# Baseline Benchmark Results Summary

This file is a template for measured baseline results before implementing the Redis/Memcached-compatible endpoint.

## Benchmark Date

TODO: YYYY-MM-DD

## Machine Configuration

- CPU: TODO
- RAM: TODO
- Disk: TODO
- OS: TODO
- ClickHouse commit: TODO
- Redis version: TODO
- Build type: TODO

## Dataset Description

- Number of keys: TODO
- Key format: `key_000000000`
- Value size: TODO
- Distribution of requested keys: TODO
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
| TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO |
| TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO |
| TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO |

## Preliminary Interpretation

- What the numbers show: TODO
- Whether SQL point lookup overhead is visible: TODO
- How Redis reference compares: TODO
- What should be checked in the final before/after benchmark: TODO

## How These Results Motivate the Implementation

TODO: Explain how the baseline results justify implementing a specialized read-only Redis-compatible endpoint over `IKeyValueEntity`. Focus on measured overhead, batching behavior, and p95/p99 latency rather than only average latency.

## Notes

Raw generated datasets and large CSV outputs should not be committed. Keep only concise summaries, scripts if needed later, and small representative result tables.
