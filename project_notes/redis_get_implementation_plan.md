# Redis `GET` Over `IKeyValueEntity` Implementation Plan

## Goal

Implement `GET <key>` for the currently selected Redis DB mapping.

## Initial Supported Scope

- Single-column primary key only.
- `String` key only.
- `String` `default_column` only.
- `EmbeddedRocksDB` table first.
- Missing key returns a Redis null bulk string.
- `GET` before successful `SELECT` returns a Redis error.

Out of scope for this stage:

- Composite primary keys.
- Non-`String` key types.
- Non-`String` value columns.
- `MGET`.
- Automatic fallback to SQL query execution.
- Production-code changes outside the Redis handler path unless an inspected API requires a small supporting change.

## Expected Flow

1. Client sends `SELECT 0`.
2. `RedisHandler` stores the configured `database`, `table`, and `default_column`.
3. Client sends `GET key`.
4. `RedisHandler` resolves the ClickHouse table from the selected mapping.
5. `RedisHandler` verifies that the table implements `IKeyValueEntity`.
6. `RedisHandler` builds `ColumnsWithTypeAndName` for the single `String` key.
7. `RedisHandler` calls `getByKeys`.
8. `RedisHandler` reads `default_column` from the returned `Chunk`.
9. `RedisHandler` writes a Redis bulk string for an existing key, or a Redis null bulk string for a missing key.

## Files Likely Changed

- `src/Server/RedisHandler.h`
- `src/Server/RedisHandler.cpp`
- Maybe `project_notes/redis_get_status.md` later, after implementation and verification.

## ClickHouse APIs To Inspect

- `src/Interpreters/IKeyValueEntity.h`
- `src/Storages/RocksDB/StorageEmbeddedRocksDB.h`
- `src/Storages/RocksDB/StorageEmbeddedRocksDB.cpp`
- `src/Dictionaries/IDictionary.h`
- `DatabaseCatalog` table lookup APIs.
- `Block`, `Chunk`, and `Column` APIs needed to read a `String` value from the returned data.

Inspection questions:

- What exact signature and return contract does `IKeyValueEntity::getByKeys` provide?
- How does `StorageEmbeddedRocksDB` represent absent keys in the returned `Chunk`?
- Which input column names and types does `getByKeys` expect for a single-column primary key?
- Does `getByKeys` require a query `Context`, settings, or a table metadata snapshot?
- How should `RedisHandler` safely access a table object from `database` and `table` names?
- How should the returned `Chunk` be matched back to the requested key for single-key `GET`?

## Expected Redis Command Behavior

- `GET key` after `SELECT 0` and an existing key returns a Redis bulk string.
- `GET missing_key` after `SELECT 0` returns a Redis null bulk string.
- `GET key` before successful `SELECT` returns a Redis error.
- `GET` with the wrong number of arguments returns a wrong-number-of-arguments Redis error.
- `GET a b` returns a wrong-number-of-arguments Redis error.
- `GET key` when the selected target table does not exist returns a Redis error.
- `GET key` when the selected target table does not implement `IKeyValueEntity` returns a Redis error.

Suggested error behavior:

- Keep protocol parsing errors as protocol errors.
- Return Redis `-ERR ...` responses for command validation, missing selection, missing table, unsupported table engine, and unsupported key or value types.
- Avoid exposing internal C++ exception details directly unless they are already sanitized by the Redis handler pattern.

## Verification Commands

```bash
redis-cli -p 9006 SELECT 0
```

```bash
redis-cli -p 9006 GET key_000000001
```

```bash
redis-cli -p 9006 GET missing_key
```

```bash
redis-cli -p 9006 GET
```

```bash
redis-cli -p 9006 GET a b
```

Additional checks if convenient:

```bash
redis-cli -p 9006 GET key_before_select
```

Run this on a fresh connection before `SELECT 0`.

## Issue Log Format

For any compile or runtime issue during this stage, record it in `project_notes/redis_get_implementation_issues.md` using this format:

```markdown
## Issue Title

- Symptom:
- Root cause:
- Fix:
- Verification:
```
