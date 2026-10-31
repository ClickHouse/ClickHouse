# Redis Limits Implementation Plan

## Goal

Add minimal protective limits to the Redis-compatible endpoint to avoid unbounded memory usage from huge or malformed RESP requests.

## Limits To Implement In This Stage

- Max array elements.
- Max bulk string size.
- Max key size.
- Max `MGET` keys.

## Limits Not Implemented In This Stage

- Total max command size.

The current parser does not track aggregate command bytes. For this MVP, max array elements, max bulk string size, key size, and `MGET` key count provide meaningful protection. Total command size can be future work if the endpoint needs configurable production hardening.

## Suggested Initial Values

Use hardcoded constants for now:

- Max array elements: `1024`.
- Max bulk string size: `1 MiB`.
- Max key size: `64 KiB`.
- Max `MGET` keys: `1024`.

## Error Behavior

Parser-level limits:

- Array too large -> `ERR Protocol error` or `ERR request array is too large`, depending on existing parser error style.
- Bulk string too large -> `ERR Protocol error` or `ERR bulk string is too large`.

Handler-level limits:

- `GET` key too large -> `ERR key is too large`.
- `MGET` key too large -> `ERR key is too large`.
- `MGET` too many keys -> `ERR too many keys for 'mget' command`.

## Scope

- Do not add new Redis commands.
- Do not change valid `GET`/`MGET` behavior.
- Do not change `IKeyValueEntity` usage.
- Do not add config plumbing in this stage.
- Do not refactor unrelated code.
- Do not implement auth/security.

## Files Likely Changed

- `src/Server/RedisProtocol.cpp`
- `src/Server/RedisProtocol.h` only if needed.
- `src/Server/RedisHandler.cpp`
- `src/Server/RedisHandler.h` only if needed.
- `tests/integration/test_redis_protocol/test.py`
- `project_notes/redis_limits_implementation_plan.md`

## Test Plan

- `GET` with key larger than max key size -> Redis error, then `PING` works.
- `MGET` with one key larger than max key size -> Redis error, then `PING` works.
- `MGET` with more than max `MGET` keys -> Redis error, then `PING` works.
- RESP array with more than max array elements -> Redis error or safe connection close.
- Bulk string length larger than max bulk string size -> Redis error or safe connection close.
- Server accepts a new connection and `PING` works after parser-level limit errors.

## Verification

- Build `clickhouse-server-lib` if available.
- Run:

```bash
python3 -m py_compile tests/integration/test_redis_protocol/test.py
```

- Remove generated cache:

```bash
rm -rf tests/integration/test_redis_protocol/__pycache__
```

- Run integration tests if Docker/pytest are available.
- If the integration runner is unavailable, report it clearly.
