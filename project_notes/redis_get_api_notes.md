# Redis `GET` API Notes

## Relevant Existing APIs

`IKeyValueEntity` lives in `src/Interpreters/IKeyValueEntity.h`.

Important methods:

```cpp
virtual Names getPrimaryKey() const = 0;

virtual Chunk getByKeys(
    const ColumnsWithTypeAndName & keys,
    const Names & required_columns,
    PaddedPODArray<UInt8> & out_null_map,
    IColumn::Offsets & out_offsets) const = 0;

virtual Block getSampleBlock(const Names & required_columns) const = 0;
```

`StorageEmbeddedRocksDB` implements `IKeyValueEntity` directly:

- Header: `src/Storages/RocksDB/StorageEmbeddedRocksDB.h`
- Implementation: `src/Storages/RocksDB/StorageEmbeddedRocksDB.cpp`
- Class declaration: `class StorageEmbeddedRocksDB final : public StorageWithCommonVirtualColumns, public IKeyValueEntity, WithContext`

Other implementations and users inspected:

- `src/Dictionaries/IDictionary.h`
- `src/Storages/StorageRedis.cpp`
- `src/Storages/StorageKeeperMap.cpp`
- `src/Interpreters/DirectJoin.cpp`
- `src/Interpreters/JoinedTables.cpp`
- `src/Planner/PlannerJoins.cpp`
- `src/Planner/PlannerJoinsLogical.cpp`

## Exact Include Files Likely Needed

For `RedisHandler.cpp`:

```cpp
#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Interpreters/StorageID.h>
#include <Processors/Chunk.h>
#include <Storages/IStorage.h>
#include <Common/PODArray.h>
#include <Common/typeid_cast.h>
```

Some of these may already be transitively included, but the implementation should include direct dependencies.

Likely standard includes:

```cpp
#include <string_view>
```

## Table Lookup API

Use `DatabaseCatalog` with a non-empty `StorageID`:

```cpp
ContextPtr context = server.context();
StoragePtr storage = DatabaseCatalog::instance().getTable(
    StorageID(selected_target.database, selected_target.table),
    context);
```

`getTable` throws if the database or table does not exist. If the Redis path wants to convert missing target tables into Redis `-ERR ...`, catch the ClickHouse exception around lookup and write a Redis error response.

`tryGetTable` is also available:

```cpp
StoragePtr storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);
```

For `GET`, `getTable` is probably better because it preserves the exact ClickHouse lookup exception for logging while the Redis handler can still sanitize the client response.

## How To Get `StoragePtr`

`StoragePtr` is returned directly by `DatabaseCatalog::instance().getTable`.

The current `RedisHandler` has `IServer & server`; `IServer::context` returns `ContextMutablePtr`, which can be assigned to `ContextPtr`.

Expected shape:

```cpp
auto context = server.context();
StorageID storage_id(selected_target.database, selected_target.table);
StoragePtr storage = DatabaseCatalog::instance().getTable(storage_id, context);
```

## How To Dynamic Cast To `IKeyValueEntity`

Existing code uses `std::dynamic_pointer_cast`:

```cpp
auto storage_kv = std::dynamic_pointer_cast<IKeyValueEntity>(storage);
```

For read-only use, a const pointer is preferable after the cast:

```cpp
std::shared_ptr<const IKeyValueEntity> key_value = std::dynamic_pointer_cast<IKeyValueEntity>(storage);
```

Planner code also uses:

```cpp
std::dynamic_pointer_cast<const IKeyValueEntity>(storage_dictionary->getDictionary())
```

For `RedisHandler`, the target is a `StoragePtr`, so casting the storage pointer as `IKeyValueEntity` is the relevant pattern.

If the cast returns null, the Redis command should return an error like target table does not implement `IKeyValueEntity`.

## How To Get Primary Key Names

Use:

```cpp
Names primary_keys = key_value->getPrimaryKey();
```

For the initial `GET` scope, require:

```cpp
primary_keys.size() == 1
```

`StorageEmbeddedRocksDB::getPrimaryKey` returns the stored `primary_keys` vector.

## How To Build A One-Row `String` Key Column

Use `ColumnString` and `DataTypeString`:

```cpp
auto key_column = ColumnString::create();
key_column->insertData(key.data(), key.size());

ColumnsWithTypeAndName keys;
keys.push_back({
    std::move(key_column),
    std::make_shared<DataTypeString>(),
    primary_keys.front()
});
```

The key column name should be the ClickHouse primary key column name from `getPrimaryKey`.

Before calling `getByKeys`, validate that the table primary key type is exactly `String`. The most direct way is to inspect the storage sample block:

```cpp
Block sample_block = key_value->getSampleBlock({});
const auto & primary_key_sample = sample_block.getByName(primary_keys.front());
```

Then check the type. Options:

```cpp
primary_key_sample.type->getTypeId() == TypeIndex::String
```

or:

```cpp
isString(primary_key_sample.type)
```

`StorageEmbeddedRocksDB::getByKeys` compares key types after removing `Nullable` and `LowCardinality` wrappers, but this Redis stage should reject wrappers unless they are intentionally added to the supported scope.

## How To Call `getByKeys`

Use the selected `default_column` as the only required result column:

```cpp
Names required_columns{selected_target.default_column};
PaddedPODArray<UInt8> found_map;
IColumn::Offsets offsets;

Chunk chunk = key_value->getByKeys(keys, required_columns, found_map, offsets);
```

For `StorageEmbeddedRocksDB`, `required_columns` is currently ignored and `getSampleBlock` also ignores it, returning the full table sample block. Dictionaries do honor `result_names`. The Redis code should still pass `{default_column}` because it is the correct interface usage.

For this stage, reject or error if `offsets` is not empty. `StorageEmbeddedRocksDB` ignores `out_offsets`, so it should stay empty.

## How To Interpret `out_null_map`

Important caveat: the name and `IKeyValueEntity` comment say null/missing map, but the implementations inspected use `1` for found and `0` for missing.

Evidence:

- `StorageEmbeddedRocksDB::getByKeys` initializes `null_map` with `1` and sets an entry to `0` for `rocksdb::Status::IsNotFound`.
- `StorageRedis::getBySerializedKeys` initializes `null_map` with `1` and sets an entry to `0` for null Redis bulk string.
- `IDictionary::getByKeys` copies `hasKeys`, whose documented behavior is `1` when the key is in the dictionary and `0` otherwise.
- `DirectJoin` passes this map to `filterWithBlanks`; that helper keeps source values for `1` and inserts defaults for `0`.

For Redis `GET`:

```cpp
if (found_map.empty() || !found_map[0])
    write null bulk string;
else
    read row 0 from default_column and write bulk string;
```

Also verify `chunk.getNumRows() == 1` for the single-key path. If the chunk has zero rows or more than one row, return a Redis error until broader semantics are designed.

## How To Read `default_column` From Returned `Chunk`

`Chunk` stores only columns, not names. Use a `Block` sample from `getSampleBlock` to map names to positions:

```cpp
Block result_sample = key_value->getSampleBlock(required_columns);
size_t value_pos = result_sample.getPositionByName(selected_target.default_column);
const auto & value_type = result_sample.getByPosition(value_pos).type;
const auto & value_column = chunk.getColumns().at(value_pos);
```

For `EmbeddedRocksDB`, because `getSampleBlock(required_columns)` returns the full table sample block and `getByKeys` returns full table columns, this position mapping is expected to line up.

Validate the `default_column` type is exactly `String`:

```cpp
value_type->getTypeId() == TypeIndex::String
```

Then read the value:

```cpp
const auto * string_column = typeid_cast<const ColumnString *>(value_column.get());
if (!string_column)
    return Redis error;

std::string_view value = string_column->getDataAt(0);
RedisProtocol::writeBulkString(*out, String(value));
```

`ColumnString::getDataAt` returns `std::string_view`, and `ColumnString::insertData` accepts raw bytes. This is suitable for Redis bulk strings because values may contain arbitrary bytes; avoid SQL-style escaping or text serialization for the initial `String`-only scope.

## Field Or Column Serialization To `String`

There are general serialization APIs through `IDataType::getDefaultSerialization` and methods such as `serializeText`, `serializeTextRaw`, and `serializeBinary`. Existing examples include MySQL packet formatting and format escaping utilities.

Do not use generic text serialization for this stage if `default_column` is `String`: it can introduce quoting or escaping depending on the method. Reading `ColumnString::getDataAt(0)` is the direct byte-preserving path.

For future non-`String` values, use a deliberate Redis representation decision and then wire it through an explicit serialization method. Do not implicitly call debug-oriented `FieldVisitorToString` for Redis responses.

## Expected Implementation Skeleton

```cpp
if (!has_selected_target)
{
    RedisProtocol::writeError(*out, "ERR Redis DB is not selected");
    return;
}

ContextPtr context = server.context();
StoragePtr storage = DatabaseCatalog::instance().getTable(
    StorageID(selected_target.database, selected_target.table),
    context);

auto key_value = std::dynamic_pointer_cast<IKeyValueEntity>(storage);
if (!key_value)
{
    RedisProtocol::writeError(*out, "ERR target table does not support key-value lookup");
    return;
}

Names primary_keys = key_value->getPrimaryKey();
if (primary_keys.size() != 1)
{
    RedisProtocol::writeError(*out, "ERR only single-column primary key is supported");
    return;
}

Names required_columns{selected_target.default_column};
Block sample = key_value->getSampleBlock(required_columns);
size_t key_pos = sample.getPositionByName(primary_keys.front());
size_t value_pos = sample.getPositionByName(selected_target.default_column);

if (sample.getByPosition(key_pos).type->getTypeId() != TypeIndex::String ||
    sample.getByPosition(value_pos).type->getTypeId() != TypeIndex::String)
{
    RedisProtocol::writeError(*out, "ERR only String key and value columns are supported");
    return;
}

auto key_column = ColumnString::create();
key_column->insertData(key.data(), key.size());

ColumnsWithTypeAndName keys{
    {std::move(key_column), std::make_shared<DataTypeString>(), primary_keys.front()}
};

PaddedPODArray<UInt8> found_map;
IColumn::Offsets offsets;
Chunk chunk = key_value->getByKeys(keys, required_columns, found_map, offsets);

if (!offsets.empty() || chunk.getNumRows() != 1 || found_map.size() != 1)
{
    RedisProtocol::writeError(*out, "ERR unexpected key-value lookup result");
    return;
}

if (!found_map[0])
{
    RedisProtocol::writeNullBulkString(*out);
    return;
}

const auto * value_column = typeid_cast<const ColumnString *>(chunk.getColumns().at(value_pos).get());
if (!value_column)
{
    RedisProtocol::writeError(*out, "ERR value column is not String");
    return;
}

std::string_view value = value_column->getDataAt(0);
RedisProtocol::writeBulkString(*out, String(value));
```

This skeleton assumes `RedisProtocol` has or will get a null bulk string writer. If not, `RedisHandler` needs to write `$-1\r\n` through the existing output buffer or add a small protocol helper.

## Uncertainty And Risks

- `RedisHandler` currently has `selected_db` and `selected_target` but no explicit `has_selected_target` flag. Since `selected_db` defaults to `0`, `GET` before `SELECT` needs a separate boolean or an equivalent check.
- `IKeyValueEntity` documentation says `out_null_map` indicates missing keys, but implementations and direct join behavior use `1` for found and `0` for missing. The implementation should follow existing behavior and maybe add a local variable name like `found_map` to avoid confusion.
- `StorageEmbeddedRocksDB::getByKeys` ignores `required_columns`, so reading by position must use the same `getSampleBlock(required_columns)` source. That works today because both return the full table layout, but this relies on implementation consistency.
- `ColumnString` direct casting will fail for `LowCardinality(String)` or `Nullable(String)`. That is acceptable for the initial exact-`String` scope.
- `DatabaseCatalog::getTable` may throw exceptions for missing databases/tables or access/context issues. The Redis handler should catch these around command handling and return sanitized Redis errors instead of closing the connection for ordinary command failures.
- `getByKeys` may throw for primary key type mismatch or storage-level errors. Redis `GET` should convert expected validation failures to `-ERR ...`; storage exceptions can be logged and returned as a generic Redis error.
- Redis bulk strings are byte strings. Converting `std::string_view` to `String` copies bytes and should preserve embedded NUL bytes, but the existing `RedisProtocol::writeBulkString` should be checked to ensure it writes by size, not C-string termination.
