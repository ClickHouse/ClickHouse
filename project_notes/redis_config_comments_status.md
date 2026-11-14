# Redis Config Comments Status

- Date: 2026-05-11
- Branch: `redis-handler`
- Current short SHA: `15f5331e97e`

## Goal

Keep Redis endpoint config examples and comments aligned with the current implementation.

## Files Inspected

- `programs/server/config.xml`
- `programs/server/config.yaml.example`
- `tests/integration/test_redis_protocol/configs/redis.xml`

## Files Changed

- `programs/server/config.xml`
- `programs/server/config.yaml.example`

## Files Intentionally Not Changed

- `tests/integration/test_redis_protocol/configs/redis.xml`, because it has active test mappings and no stale comments.

## Current Documented Scope

- Redis-compatible read-only endpoint.
- `PING`, `QUIT`, `SELECT`, `GET`, `MGET`.
- `IKeyValueEntity::getByKeys`.
- Redis DB mapping to ClickHouse `database`/`table`/`default_column`.
- `String`/`UInt64` key support.
- `String` `default_column` value.
- Not a full Redis implementation.
- Not a Redis replacement.
- Not for arbitrary ClickHouse tables.

## Verification

- Comments only.
- No runtime behavior changes.
- No C++ code changes.
- No benchmarks run.

## Next Stage

- Benchmark summary update.
