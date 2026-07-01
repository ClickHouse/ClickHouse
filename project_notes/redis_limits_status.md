# Redis Protective Limits Status

- Date: 2026-05-11
- Branch: `redis-handler`
- Current short SHA: `c8513dcfe24`

## Files Changed

- `src/Server/RedisProtocol.cpp`
- `src/Server/RedisHandler.cpp`
- `tests/integration/test_redis_protocol/test.py`
- `project_notes/redis_limits_implementation_plan.md`

## Limits Implemented

Parser-level:

- `MAX_ARRAY_ELEMENTS = 2048`
- `MAX_BULK_STRING_SIZE = 1 MiB`

Handler-level:

- `MAX_KEY_SIZE = 64 KiB`
- `MAX_MGET_KEYS = 1024`

## Not Implemented In This Stage

- Total command size limit.
- Config-based limit settings.

Total command size was left as future work because the current parser does not track aggregate command bytes. The combination of array element, bulk string, key size, and `MGET` count limits provides useful MVP protection.

## Error Behavior

- RESP array too large uses the existing protocol error path.
- RESP bulk string too large uses the existing protocol error path and is checked before `String::resize`.
- `GET` key too large returns Redis error: `ERR key is too large`.
- `MGET` key too large returns Redis error: `ERR key is too large`.
- `MGET` too many keys returns Redis error: `ERR too many keys for 'mget' command`.

## Tests Added

- `test_get_key_too_large`
- `test_mget_key_too_large`
- `test_mget_too_many_keys`
- `test_resp_array_too_large`
- `test_resp_bulk_string_too_large`

## Verification Performed

- C++ build passed for `clickhouse-server-lib`:
  `ninja -C build-new clickhouse-server-lib > build-new/redis_limits_verify_build.log 2>&1`
- `py_compile` passed for `tests/integration/test_redis_protocol/test.py`:
  `python3 -m py_compile tests/integration/test_redis_protocol/test.py`
- Generated `__pycache__` was removed.
- No `*.pyc` remains under `tests/integration/test_redis_protocol`.
- Build logs are ignored under `build-new`.
- Full integration tests were not executed locally because `pytest` and Docker are unavailable.

## Important Test Behavior

- Parser-level limit tests currently expect an error response before connection close.
- This matches current `RedisHandler` protocol error behavior.
- If future protocol error handling changes to immediate close without response, those tests may need to be relaxed to accept safe close.

## Remaining Limitations

- Full integration tests still need to be run in a proper ClickHouse integration test environment.
- Total command size limit is not implemented.
- Limits are hardcoded MVP constants, not config settings.

## Next Stage

- Config comments cleanup.
- Benchmark summary update.
