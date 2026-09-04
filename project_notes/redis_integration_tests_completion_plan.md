# Redis Integration Tests Completion Plan

## Goal

Extend the existing Redis protocol integration tests to cover `UInt64` key support and unsupported schema errors.

## Scope

Add tests for:

- `UInt64` `GET` existing key.
- `UInt64` `GET` missing key.
- `UInt64` `MGET` existing keys.
- `UInt64` `MGET` mixed existing/missing keys.
- Invalid `UInt64` key: `abc`.
- Invalid `UInt64` key: `-1`.
- Invalid `UInt64` overflow.
- Unsupported non-`IKeyValueEntity` table.
- Unsupported key type.
- Unsupported value/default column type.
- Server survives after these errors.

## Out Of Scope

- No new Redis commands.
- No benchmark changes.
- No C++ changes unless tests reveal a real bug.
- No broad refactor of existing tests.
- No production limits in this stage.

## Expected Redis DB Mappings

- DB `0`: existing `String` key table `default.kv_baseline`.
- DB `1`: `UInt64` key table `default.kv_uint64`.
- DB `2`: non-`IKeyValueEntity` table `default.kv_not_key_value`.
- DB `3`: unsupported key type table `default.kv_uint32_key`.
- DB `4`: unsupported value type table `default.kv_uint64_value`.

## Test Data

Use small deterministic tables.

`kv_uint64`:

- `key UInt64`
- `value String`
- `0 -> value_0`
- `65536 -> value_65536`
- `131072 -> value_131072`

Unsupported tables should contain minimal data, only enough to trigger the intended errors.

## Files Likely Changed

- `tests/integration/test_redis_protocol/test.py`
- `tests/integration/test_redis_protocol/configs/redis.xml`
- `project_notes/redis_integration_tests_completion_plan.md`

## Existing Helpers To Reuse

- `prepare_table`
- `encode_command`
- `read_exact`
- `read_line`
- `read_response`
- `connect_redis`
- `command`
- `assert_error`

## Implementation Order

1. Add Redis DB mappings to `redis.xml`.
2. Extend `prepare_table` to create new tables.
3. Add `UInt64` `GET`/`MGET` tests.
4. Add invalid `UInt64` key tests.
5. Add unsupported schema tests.
6. Add server liveness checks after errors.
7. Run `py_compile`.
8. Remove generated `__pycache__`.
9. Run integration tests if the runner is available.

## Verification

Run:

```bash
python3 -m py_compile tests/integration/test_redis_protocol/test.py
```

Remove generated cache if created:

```bash
rm -rf tests/integration/test_redis_protocol/__pycache__
```

Run integration tests if available:

```bash
pytest tests/integration/test_redis_protocol
```

If the integration test runner or environment is unavailable, report it clearly.

## Completion Criteria

- `UInt64` `GET`/`MGET` covered.
- Invalid `UInt64` keys covered.
- Unsupported schema cases covered.
- Server liveness after errors covered.
- Syntax check passes.
- No generated garbage included.
