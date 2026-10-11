# Redis Benchmark Documentation Status

- Date: 2026-05-11
- Branch: `redis-handler`
- Current short SHA: `d8af3286268`

## Goal

Update benchmark documentation for final thesis use.

## Files Updated

- `project_notes/redis_final_benchmark_results_summary.md`

## Files Inspected But Not Changed

- `project_notes/redis_final_benchmark_validity_review.md`

## What Is Now Documented

- Research question framing.
- 100k `String`-key benchmark.
- ClickHouse HTTP SQL path.
- ClickHouse Redis-compatible endpoint.
- Redis raw reference baseline.
- 10M-row `bench.kv_test` `UInt64` benchmark.
- QPS per command/request.
- `MGET` key throughput as a derived metric.
- p99 latency interpretation.
- Hot-cache local-loopback caveat.
- Limitations.
- Careful conclusion / answer to research question.

## Important Wording

- Do not claim ClickHouse is faster than Redis.
- Do not claim production-wide performance.
- Do not claim full Redis compatibility.
- Say that Redis-compatible endpoint removes much of SQL/HTTP overhead for supported point lookups over prepared key-value tables.

## Verification

- Docs only.
- No benchmarks run.
- No benchmark scripts changed.
- No C++ code changes.

## Next Stage

- Thesis/report structure.
- Presentation/defense script.
- Final repository hygiene.
