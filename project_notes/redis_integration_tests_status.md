# Redis Integration Tests Status

Date: 2026-05-08

Current short SHA: `5852540fbf1`

## Test Directory

```text
tests/integration/test_redis_protocol
```

Files:

```text
tests/integration/test_redis_protocol/__init__.py
tests/integration/test_redis_protocol/test.py
tests/integration/test_redis_protocol/configs/redis.xml
```

## Covered Behavior

The integration test file covers:

- `PING`
- `QUIT`
- `SELECT` valid DB
- `SELECT` invalid DB
- `SELECT` invalid index
- `GET` existing key
- `GET` missing key
- `GET` wrong arity
- `GET` before `SELECT`
- `MGET` existing keys
- `MGET` mixed existing/missing
- `MGET` wrong arity
- `MGET` before `SELECT`
- server survives errors

## Test Backend

- `EmbeddedRocksDB`
- `key String`
- `value String`

## Test Command And Result

Attempted command:

```bash
python3 -m ci.praktika run "integration" --test test_redis_protocol --path ./build-new/programs/clickhouse > build-new/test_redis_protocol.log 2>&1
```

Result: not executed successfully in this local environment. The command failed before starting the integration test container, so no Redis protocol test failures were observed.

Direct `pytest` is unavailable:

```text
/usr/bin/python3: No module named pytest
```

The Praktika integration runner also cannot run because Docker is unavailable:

```text
/bin/bash: line 1: docker: command not found
/bin/sh: 1: docker: not found
```

## Remaining Requirement

Run the tests in CI or in a proper ClickHouse integration environment with:

- Docker available;
- Python test dependencies installed, including `pytest`;
- a ClickHouse binary available to the integration runner.

Suggested command:

```bash
python3 -m ci.praktika run "integration" --test test_redis_protocol --path ./build-new/programs/clickhouse
```

## Issues Recorded

`project_notes/redis_integration_tests_issues.md` does not exist yet, and no stage-specific integration test issue has been recorded.

## Next Planned Stage

- Final before/after benchmark.
- Optional limits and pipelining.
