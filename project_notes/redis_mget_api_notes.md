# Redis `MGET` API Notes

## Current `GET` Target Resolution

`RedisHandler::selectDatabase` parses the `SELECT` argument as an unsigned DB number and reads the selected target from server config:

```cpp
String prefix = "redis.db._" + std::to_string(parsed_db);
target.database = config.getString(prefix + ".database", "");
target.table = config.getString(prefix + ".table", "");
target.default_column = config.getString(prefix + ".default_column", "");
```

If all three values are present, it stores them in `selected_target`, sets `selected_db`, and sets `has_selected_target = true`.

`RedisHandler::getKey` checks `has_selected_target` first. If no successful `SELECT` happened on the connection, it writes:

```text
ERR no Redis DB selected
```

After that, `GET` resolves the table with:

```cpp
ContextPtr context = server.context();
StoragePtr storage = DatabaseCatalog::instance().getTable(
    StorageID(selected_target.database, selected_target.table),
    context);
```

The table is cast with:

```cpp
auto key_value = std::dynamic_pointer_cast<IKeyValueEntity>(storage);
```

If the cast fails, the command writes:

```text
ERR target table does not support key-value lookup
```

## `GET` Parts Reusable For `MGET`

The following `GET` logic should be reused or kept structurally identical for `MGET`:

- no selected target check;
- `DatabaseCatalog` table lookup;
- `IKeyValueEntity` cast;
- `getPrimaryKey` validation for exactly one key column;
- `getSampleBlock(required_columns)` to map key and value column positions;
- key type validation as exactly `String`;
- `default_column` type validation as exactly `String`;
- `required_columns{selected_target.default_column}`;
- `getByKeys` call with `PaddedPODArray<UInt8>` and `IColumn::Offsets`;
- `Exception` handling that logs and writes a Redis error without closing the connection.

The main difference is that `MGET` should build one `ColumnString` with all requested keys and then write an array response with one element per input key.

Avoid broad refactoring in this stage. A small shared helper is reasonable only if it reduces direct duplication around table lookup and type validation.

## Building A `ColumnString` With Multiple Keys

Use the same `ColumnString` API as `GET`, but insert every Redis key argument:

```cpp
auto key_column = ColumnString::create();
for (const auto & key : keys_to_get)
    key_column->insertData(key.data(), key.size());
```

Then build the `ColumnsWithTypeAndName` with one key column:

```cpp
ColumnsWithTypeAndName keys;
keys.push_back({
    std::move(key_column),
    std::make_shared<DataTypeString>(),
    primary_keys.front()
});
```

For the initial scope, reject non-`String` primary keys before building or using the key column.

## Calling `getByKeys` Once

Call `getByKeys` once for the full set of input keys:

```cpp
Names required_columns{selected_target.default_column};
PaddedPODArray<UInt8> found_map;
IColumn::Offsets offsets;
Chunk chunk = key_value->getByKeys(keys, required_columns, found_map, offsets);
```

`IKeyValueEntity::getByKeys` documents two result shapes:

- if `out_offsets` is empty, the number of rows in the returned `Chunk` equals the number of input rows;
- if `out_offsets` is not empty, row count can differ due to `ALL` join semantics.

`MGET` needs one response element per input key, so the first version should require `offsets.empty()`.

## Input Keys, `Chunk` Rows, And `found_map`

For `StorageEmbeddedRocksDB::getByKeys`:

- `num_rows` is taken from `keys[0].column->size`;
- `null_map` is resized to `num_rows` and filled with `1`;
- missing RocksDB keys set `null_map[i] = 0`;
- the returned block contains one row per input key;
- missing rows are filled with default values.

Despite the parameter name `out_null_map`, existing behavior uses `1` for found and `0` for missing. `IDictionary::getByKeys` copies `hasKeys`, which follows the same found-mask convention. `DirectJoin` also treats this map as a keep/default mask.

For `MGET`, expected validation before writing the response:

```cpp
const size_t requested = command.arguments.size();
if (!offsets.empty() || chunk.getNumRows() != requested || found_map.size() != requested)
    write Redis error;
```

When valid:

- row `i` in the result corresponds to input key `i`;
- `found_map[i] == 1` means write the value from row `i`;
- `found_map[i] == 0` means write a Redis null bulk string for element `i`.

## Reading Result Values

`Chunk` stores columns without names. Use the `Block` returned by `getSampleBlock(required_columns)` to map `selected_target.default_column` to a column position:

```cpp
Block sample_block = key_value->getSampleBlock(required_columns);
size_t value_pos = sample_block.getPositionByName(selected_target.default_column);
```

Then validate the returned chunk has that position:

```cpp
if (chunk.getNumColumns() <= value_pos)
    write Redis error;
```

For the initial scope, require the returned value column to be `ColumnString`:

```cpp
const auto * value_column = typeid_cast<const ColumnString *>(chunk.getColumns()[value_pos].get());
if (!value_column)
    write Redis error;
```

For each found row:

```cpp
std::string_view value = value_column->getDataAt(i);
RedisProtocol::writeBulkString(out, value);
```

This preserves raw `String` bytes and avoids generic text serialization.

## Redis Array Response

`RedisProtocol` already has:

```cpp
void writeArrayHeader(WriteBuffer & out, size_t size);
void writeBulkString(WriteBuffer & out, std::string_view value);
void writeNullBulkString(WriteBuffer & out);
```

For `MGET`, write the array header first:

```cpp
RedisProtocol::writeArrayHeader(out, requested);
```

Then write exactly one element per input key:

```cpp
for (size_t i = 0; i < requested; ++i)
{
    if (!found_map[i])
        RedisProtocol::writeNullBulkString(out);
    else
        RedisProtocol::writeBulkString(out, value_column->getDataAt(i));
}
```

Do not write a partial array if validation fails. Validate `offsets`, row count, map size, column count, and `ColumnString` type before writing the array header.

## Handling Unexpected `offsets`

`StorageEmbeddedRocksDB`, `StorageRedis`, `StorageKeeperMap`, and `IDictionary` implementations inspected leave `out_offsets` empty for their direct `getByKeys` paths. `DirectKeyValueJoin` handles non-empty offsets for `ALL` semantics, but Redis `MGET` does not have `ALL` semantics.

If `offsets` is not empty, return:

```text
ERR unexpected key-value lookup result
```

This avoids guessing how to map multiple rows back into Redis array positions.

## Handling Unexpected Row Count

If `chunk.getNumRows()` differs from the input key count, return:

```text
ERR unexpected key-value lookup result
```

Also treat `found_map.size()` mismatch as the same error. A mismatch means Redis cannot safely preserve one output element per input key.

## Keeping Errors Stable Without Closing The Connection

Follow the current `GET` pattern:

- validate command arity in `run`;
- for `MGET` with zero keys, write `ERR wrong number of arguments for 'mget' command`;
- inside the helper, write a Redis error and `return` for unsupported scope or invalid lookup result;
- catch `DB::Exception`, log at trace level, and write `ERR ` plus the exception message;
- catch all other exceptions, log with `tryLogCurrentException`, and write a generic Redis error.

Do not rethrow command execution errors from the `MGET` helper. `run` should continue reading commands on the same connection after writing the error and flushing with `out->next`.

## Uncertainty And Risks

- `IKeyValueEntity` names `out_null_map` as a null map, but practical implementations use it as a found map. Keep the variable name `found_map` in Redis code to match behavior.
- `StorageEmbeddedRocksDB::getSampleBlock(required_columns)` ignores `required_columns` and returns the full table sample block. Dictionary implementations can return only requested attributes plus keys. The position mapping should always come from the same sample block used to validate the chunk shape.
- `getByKeys` may return default values for missing rows. Redis must use `found_map`, not value contents, to decide null bulk strings.
- The `MGET` helper must validate everything before writing the Redis array header, because once the header is sent it cannot switch to a single Redis error response without corrupting the protocol response.
- Duplicate keys should naturally produce duplicate output elements because the key column contains duplicate rows and `getByKeys` returns one row per input row when `offsets` is empty.
- Empty string keys are valid Redis bulk string arguments and should be inserted into `ColumnString` as zero-length values unless a later product decision rejects them.
- This stage supports only exact `String` types. Nullable, LowCardinality, and non-String values should continue to return explicit Redis errors.
