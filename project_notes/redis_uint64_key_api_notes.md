# Redis `UInt64` Key API Notes

## Current `RedisHandler` Shape

Files inspected:

- `src/Server/RedisHandler.h`
- `src/Server/RedisHandler.cpp`
- `src/Storages/RocksDB/StorageEmbeddedRocksDB.cpp`
- `src/Storages/RocksDB/StorageEmbeddedRocksDB.h`
- `src/Storages/KVStorageUtils.h`
- `src/Storages/KVStorageUtils.cpp`
- `src/Interpreters/IKeyValueEntity.h`
- `src/Columns/ColumnsNumber.h`
- `src/DataTypes/DataTypesNumber.h`

`RedisHandler::TargetConfig` currently stores only:

- `database`
- `table`
- `default_column`

`GET` and `MGET` both resolve the selected table on every command, cast it to `IKeyValueEntity`, get the primary key with `getPrimaryKey`, and obtain a sample block with `getSampleBlock`. They require:

- exactly one primary-key column;
- primary-key type `String`;
- configured value column type `String`.

Current key construction:

- `GET` creates `ColumnString`, inserts one Redis key with `insertData`, and passes it as `DataTypeString`.
- `MGET` creates `ColumnString`, inserts every Redis key with `insertData`, and passes it as `DataTypeString`.

Both commands then call `IKeyValueEntity::getByKeys` with the constructed key column and the requested value column.

## Existing Parsing Utility In `RedisHandler`

`RedisHandler` already includes:

```cpp
#include <charconv>
#include <string_view>
#include <system_error>
```

`SELECT` already uses `std::from_chars` into `UInt64` and checks:

- parse error via `result.ec`;
- full input consumption via `result.ptr == end`;
- an earlier first-byte digit check rejects empty input and leading non-digits such as `-`.

The same style is suitable for `UInt64` Redis key parsing, but the key parser should be factored separately from `SELECT` because the error message should refer to the key, not the DB index.

## Likely Includes Needed

`RedisHandler.cpp` already has most of the needed includes. For `UInt64` key columns, likely add:

```cpp
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
```

Existing includes that should remain useful:

```cpp
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Common/typeid_cast.h>
#include <charconv>
#include <system_error>
```

`ColumnUInt64` is an alias for `ColumnVector<UInt64>` in `src/Columns/ColumnsNumber.h`. `DataTypeUInt64` is an alias for `DataTypeNumber<UInt64>` in `src/DataTypes/DataTypesNumber.h`.

## Detecting Primary-Key Type

Current code uses:

```cpp
sample_block.getByPosition(key_pos).type->getTypeId()
```

It compares against `TypeIndex::String`. Minimal extension can branch on:

```cpp
const auto key_type_id = sample_block.getByPosition(key_pos).type->getTypeId();
if (key_type_id == TypeIndex::String)
{
    ...
}
else if (key_type_id == TypeIndex::UInt64)
{
    ...
}
else
{
    RedisProtocol::writeError(out, "ERR only String or UInt64 key column is supported");
    return;
}
```

The value column check should remain strict:

```cpp
sample_block.getByPosition(value_pos).type->getTypeId() == TypeIndex::String
```

This keeps `String` value/default-column behavior unchanged.

## Building A One-Row `UInt64` Key Column

For `GET`, the intended column construction is:

```cpp
UInt64 parsed_key = 0;
if (!parseRedisUInt64Key(key, parsed_key))
{
    RedisProtocol::writeError(out, "ERR invalid UInt64 key");
    return;
}

auto key_column = ColumnUInt64::create();
key_column->insertValue(parsed_key);

ColumnsWithTypeAndName keys;
keys.push_back({std::move(key_column), std::make_shared<DataTypeUInt64>(), primary_keys.front()});
```

`ColumnUInt64::create()` is used elsewhere in the codebase. For example, code in `VirtualColumnUtils` inserts `ColumnUInt64::create()` with `std::make_shared<DataTypeUInt64>()`, and patch-part code uses `ColumnUInt64::create(num_rows, value)` for prefilled numeric columns.

## Building A Multi-Row `UInt64` Key Column

For `MGET`, the intended column construction is:

```cpp
auto key_column = ColumnUInt64::create();
for (const auto & key : keys_to_get)
{
    UInt64 parsed_key = 0;
    if (!parseRedisUInt64Key(key, parsed_key))
    {
        RedisProtocol::writeError(out, "ERR invalid UInt64 key");
        return;
    }
    key_column->insertValue(parsed_key);
}

ColumnsWithTypeAndName keys;
keys.push_back({std::move(key_column), std::make_shared<DataTypeUInt64>(), primary_keys.front()});
```

If any key is invalid, return one Redis `-ERR` for the command and do not call `getByKeys`.

## Safe Redis Key Parsing To `UInt64`

Recommended helper shape:

```cpp
static bool parseRedisUInt64Key(const String & key, UInt64 & value)
{
    if (key.empty())
        return false;

    if (key.front() < '0' || key.front() > '9')
        return false;

    const char * begin = key.data();
    const char * end = key.data() + key.size();
    auto result = std::from_chars(begin, end, value);
    return result.ec == std::errc{} && result.ptr == end;
}
```

This rejects:

- empty keys;
- negative keys such as `-1`;
- keys with plus signs such as `+1`;
- non-numeric keys such as `abc`;
- partial parses such as `123abc`;
- overflow such as `18446744073709551616`.

The helper intentionally accepts decimal digits only. Leading zeroes such as `000123` would parse as `123`; that seems acceptable unless the endpoint wants to reject non-canonical decimal spelling.

## Keeping `String` Key Behavior Unchanged

Do not parse keys on the `String` path. Keep the current `ColumnString` construction:

```cpp
auto key_column = ColumnString::create();
key_column->insertData(key.data(), key.size());
```

For `MGET`, keep inserting each input key byte string exactly as received. Invalid numeric strings such as `abc`, `-1`, or `18446744073709551616` should still be ordinary Redis keys when the configured ClickHouse primary key is `String`.

The branch should be based only on the configured target table primary-key type.

## `IKeyValueEntity` And `EmbeddedRocksDB` Behavior

`IKeyValueEntity::getByKeys` accepts `ColumnsWithTypeAndName` for the key columns and returns a `Chunk` plus `found_map` and `offsets`.

`StorageEmbeddedRocksDB::getByKeys`:

- checks that the number of key columns matches its primary-key column count;
- unwraps nullable/low-cardinality wrappers;
- verifies that every supplied key column type equals the stored primary-key type;
- serializes each key value using the key column type's default serialization;
- calls RocksDB `MultiGet`;
- returns one output row per requested key when `offsets` is empty.

This is the key point for `UInt64`: as long as `RedisHandler` supplies a `ColumnUInt64` with `DataTypeUInt64`, `EmbeddedRocksDB` should serialize the key in the same binary format used at insert time.

`StorageEmbeddedRocksDB` supports single or multi-column primary keys generally, but the Redis endpoint should continue to enforce single-column primary keys.

## Storage Type Validation Notes

`StorageEmbeddedRocksDB` creation requires at least one primary-key column and rejects subcolumns in the primary key. It does not appear to restrict primary-key type to `String`; it records primary-key names, positions, and data types from the table metadata. Existing table `bench.kv_test` with `key UInt64` confirms that `UInt64` primary keys are accepted by the engine.

The Redis endpoint currently applies the stricter key-type restriction itself, in `RedisHandler`.

## Uncertainty And Risks

- `RedisHandler::getKey` and `RedisHandler::getKeys` currently duplicate schema lookup and key-column construction. Adding `UInt64` support in both places may increase duplication unless a small helper is introduced.
- Error wording should be stable and Redis-like, for example `ERR invalid UInt64 key`.
- `std::from_chars` must consume the full input; otherwise values like `123abc` could be accepted accidentally.
- `std::from_chars` for unsigned integers must not be the only negative-key defense; keep the leading digit check so `-1` is rejected clearly.
- Empty Redis bulk-string keys are legal in Redis generally, but they cannot represent a `UInt64` key in this mapping and should return `-ERR`.
- The `String` key path must not start rejecting keys that look invalid numerically.
- The current value-column extraction uses `value_pos` from the full sample block and then indexes `chunk.getColumns()[value_pos]`. This works with current observed behavior, but any future `IKeyValueEntity` implementation that returns only requested columns in a different order could expose an unrelated issue. This is not specific to `UInt64`.
