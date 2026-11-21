# Redis `MGET` Implementation Plan

## Goal

Implement `MGET <key1> <key2> ...` for the selected Redis DB mapping.

## Initial Supported Scope

- Single-column primary key only.
- `String` keys only.
- `String` `default_column` only.
- `EmbeddedRocksDB` table first.
- Missing keys return Redis null bulk strings.
- `MGET` before successful `SELECT` returns a Redis error.
- `MGET` with zero keys returns a wrong number of arguments error.

## Expected Flow

1. Client sends `SELECT 0`.
2. `RedisHandler` stores `database`, `table`, and `default_column`.
3. Client sends `MGET key1 key2 ...`.
4. `RedisHandler` resolves the ClickHouse table from the selected mapping.
5. `RedisHandler` verifies the table implements `IKeyValueEntity`.
6. `RedisHandler` builds one `ColumnString` containing all requested keys.
7. `RedisHandler` calls `getByKeys` once.
8. `RedisHandler` reads `default_column` from the returned `Chunk`.
9. `RedisHandler` writes a Redis array with one element per input key.
10. Found keys become Redis bulk strings.
11. Missing keys become Redis null bulk strings.

## Relationship To `GET`

- Reuse existing `GET` helper logic where possible.
- Avoid copying too much table lookup, type validation, and `getByKeys` setup code.
- Do not do broad refactoring during this stage.
- Keep the implementation scoped to minimal `MGET` support.

## Files Likely Changed

- `src/Server/RedisHandler.h`
- `src/Server/RedisHandler.cpp`
- Maybe `project_notes/redis_mget_status.md` later.

## ClickHouse APIs To Reuse

- `DatabaseCatalog` table lookup.
- `IKeyValueEntity::getPrimaryKey`.
- `IKeyValueEntity::getSampleBlock`.
- `IKeyValueEntity::getByKeys`.
- `ColumnString` for the key column.
- `Chunk` and `ColumnString` for result values.
- `out_null_map` / `found_map` behavior from `getByKeys`.

## Expected Redis Command Behavior

- `MGET key_000000001 key_000000002` after `SELECT 0` returns a Redis array of bulk strings.
- `MGET key_000000001 missing_key` returns an array containing the found value and `nil`.
- `MGET` before `SELECT` returns a Redis error.
- `MGET` with no arguments returns a wrong number of arguments error.
- `MGET` when the target table does not exist returns a Redis error.
- `MGET` when the target table does not implement `IKeyValueEntity` returns a Redis error.

## Verification Commands

```bash
printf 'SELECT 0\nMGET key_000000001 key_000000002 missing_key\n' | redis-cli -h 127.0.0.1 -p 9006
```

```bash
redis-cli -h 127.0.0.1 -p 9006 MGET
```

```bash
redis-cli -h 127.0.0.1 -p 9006 MGET key_without_select
```

## Issue Log Format

For any compile or runtime issue during this stage, record:

- Issue title.
- Symptom.
- Root cause.
- Fix.
- Verification.
