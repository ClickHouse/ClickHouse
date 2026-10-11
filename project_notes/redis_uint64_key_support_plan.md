# Redis `UInt64` Key Support Plan

## Goal

Add minimal support for Redis `GET` and `MGET` when the configured ClickHouse table has a single-column `UInt64` primary key.

The existing `String` primary-key path must keep working unchanged.

## Existing Behavior

- A table with a single `String` primary key is supported.
- Redis key bytes are inserted into a `ColumnString`.
- The configured `String` `default_column` is returned as a Redis bulk string.
- `GET` and `MGET` call `IKeyValueEntity::getByKeys`.

## New Behavior

- If the primary key type is `UInt64`, parse each Redis key argument as an unsigned integer.
- Build a `ColumnUInt64`, also available as `ColumnVector<UInt64>`.
- Pass the key column to `IKeyValueEntity::getByKeys` exactly as the current `String` path does.
- Keep response construction unchanged: the configured `String` `default_column` is returned as a Redis bulk string.

## Supported Scope

- Single-column primary key only.
- Primary key type:
  - `String`
  - `UInt64`
- Value/default column type:
  - `String` only
- Commands:
  - `GET`
  - `MGET`
- First target engine:
  - `EmbeddedRocksDB`

Other engines that expose `IKeyValueEntity` can be considered later, but the first practical test target is `bench.kv_test`.

## Error Behavior

For a table configured with a `UInt64` primary key:

- Non-numeric key, for example `abc`: Redis error.
- Negative key, for example `-1`: Redis error.
- Overflow, for example `18446744073709551616`: Redis error.
- Empty key: Redis error.

The parser should be strict:

- Decimal digits only.
- No leading sign.
- `std::from_chars` or an equivalent helper must consume the full input.
- Overflow must be detected and reported as a Redis `-ERR`.

For a table configured with a `String` primary key, behavior must remain unchanged. Any byte sequence that was previously accepted as a Redis key for the `String` path should continue to be accepted.

## APIs To Inspect

- `ColumnVector<UInt64>`
- `ColumnUInt64`
- `DataTypeUInt64`
- Existing integer parsing helpers in ClickHouse, if there is a suitable strict parser.
- `std::from_chars` as the likely minimal strict parser for unsigned decimal input.
- Current `RedisHandler` `GET` and `MGET` implementation.
- Current Redis key column construction before calling `IKeyValueEntity::getByKeys`.

## Implementation Shape

The intended change should be small and localized:

1. During table/config validation, allow the single primary-key column type to be either `String` or `UInt64`.
2. Keep requiring the configured `default_column` type to be `String`.
3. Store or derive the key type for the configured Redis database mapping.
4. In `GET` and `MGET`, build the key column based on the configured primary-key type:
   - `String`: existing `ColumnString` path.
   - `UInt64`: new `ColumnUInt64` path.
5. For `UInt64`, parse every Redis key argument before calling `getByKeys`.
6. If parsing fails, return a Redis `-ERR` response and do not call `getByKeys` for that command.
7. Call `IKeyValueEntity::getByKeys` exactly as before once the key column is constructed.

## Test Target

Existing larger table:

```sql
CREATE TABLE bench.kv_test
(
    `key` UInt64,
    `value` String,
    `extra1` UInt32,
    `extra2` Float64
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

Observed row count:

```text
10000000
```

Redis mapping needed for manual testing:

```text
redis.db._0.database=bench
redis.db._0.table=kv_test
redis.db._0.default_column=value
```

## Verification Plan

Before manual Redis endpoint checks, pick existing numeric keys from `bench.kv_test`, for example:

```sql
SELECT key, value
FROM bench.kv_test
ORDER BY key
LIMIT 5
```

Manual Redis protocol checks:

```text
SELECT 0
GET <existing_key>
```

```text
SELECT 0
MGET <existing_key_1> <existing_key_2> <missing_numeric_key>
```

Invalid key checks for `UInt64` primary key mode:

```text
GET abc
GET -1
GET 18446744073709551616
```

Expected invalid-key behavior:

- Each invalid key returns a Redis `-ERR`.
- The server remains usable after the error.
- The same invalid byte strings must still be accepted as ordinary keys when the configured table uses a `String` primary key.

Regression checks for the existing `String` key table:

```text
redis.db._0.database=default
redis.db._0.table=kv_baseline
redis.db._0.default_column=value
```

Run:

```text
SELECT 0
GET key_000000001
MGET key_000000001 key_000000002 missing_key
```

## Benchmark Plan

After implementation and manual verification, compare ClickHouse HTTP SQL against the ClickHouse Redis-compatible endpoint on `bench.kv_test`.

Dataset:

- Existing `bench.kv_test`
- 10M rows
- `UInt64` key column
- `String` value column
- `EmbeddedRocksDB`

Interfaces:

- ClickHouse HTTP SQL lookup
- ClickHouse Redis-compatible endpoint

Benchmark matrix:

- Batch sizes: 1, 10, 100
- Concurrency: 1, 8, 32
- Value/default column: `value`

Important reporting detail:

- QPS remains request/command QPS.
- For batch-size greater than 1, Redis uses `MGET` and HTTP SQL should use `WHERE key IN (...)`.
- Per-key throughput can be derived separately as request QPS multiplied by batch size.

## Risks

- Redis keys are bytes, while `UInt64` requires strict decimal parsing.
- `std::from_chars` must consume the full input; accepting prefixes such as `123abc` would be wrong.
- Negative strings must not be accepted.
- Empty keys must not be accepted for `UInt64`.
- Overflow must be detected.
- Errors must be returned as Redis `-ERR` responses.
- The existing `String` key path must not be changed semantically.
- The change should stay local to Redis key-column construction and metadata validation.
