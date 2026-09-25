#include <Common/StringUtils.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <DataTypes/Serializations/SerializationMapKeyPresence.h>
#include <DataTypes/Serializations/SerializationMapKeyPresenceMerge.h>
#include <DataTypes/Serializations/SerializationMapKeyValue.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumnsValue.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationTuple.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <Parsers/IAST.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

DataTypeMap::DataTypeMap(const DataTypePtr & nested_)
    : nested(nested_)
{
    const auto * type_array = typeid_cast<const DataTypeArray *>(nested.get());
    if (!type_array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_array->getNestedType().get());
    if (!type_tuple)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    if (type_tuple->getElements().size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    if (type_tuple->hasExplicitNames())
    {
        const auto & names = type_tuple->getElementNames();
        if (names[0] != "keys" || names[1] != "values")
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expected Tuple(key, value) with explicit names 'keys', 'values', got explicit names '{}', '{}'", names[0], names[1]);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Tuple(key, value) with explicit names 'keys', 'values', got without explicit names");

    key_type = type_tuple->getElement(0);
    value_type = type_tuple->getElement(1);
    assertKeyType();
}

DataTypeMap::DataTypeMap(const DataTypes & elems_)
{
    chassert(elems_.size() == 2);
    key_type = elems_[0];
    value_type = elems_[1];

    assertKeyType();

    nested = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{key_type, value_type}, Names{"keys", "values"}));
}

DataTypeMap::DataTypeMap(const DataTypePtr & key_type_, const DataTypePtr & value_type_)
    : key_type(key_type_), value_type(value_type_)
    , nested(std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{key_type_, value_type_}, Names{"keys", "values"})))
{
    assertKeyType();
}

void DataTypeMap::assertKeyType() const
{
    if (!isValidKeyType(key_type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Map cannot have a key of type {}", key_type->getName());
}


std::string DataTypeMap::doGetName() const
{
    WriteBufferFromOwnString s;
    s << "Map(" << key_type->getName() << ", " << value_type->getName() << ")";

    return s.str();
}

std::string DataTypeMap::doGetPrettyName(size_t indent) const
{
    WriteBufferFromOwnString s;
    s << "Map(" << key_type->getPrettyName(indent) << ", " << value_type->getPrettyName(indent) << ')';
    return s.str();
}

MutableColumnPtr DataTypeMap::createColumn() const
{
    return ColumnMap::create(nested->createColumn());
}

Field DataTypeMap::getDefault() const
{
    return Map();
}

SerializationPtr DataTypeMap::doGetSerialization(const SerializationInfoSettings & settings) const
{
    SerializationPtr key_serialization;
    SerializationPtr value_serialization;
    SerializationInfoSettings nested_settings = settings;
    /// `with_key_columns` applies to the stored Map column only. Nested Map values
    /// stay on the regular format so a value stream remains one nested Map,
    /// not another exploded key set.
    if (settings.map_serialization_version == MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS)
        nested_settings.map_serialization_version = MergeTreeMapSerializationVersion::BASIC;

    if (settings.propagate_types_serialization_versions_to_nested_types)
    {
        key_serialization = key_type->getSerialization(nested_settings);
        value_serialization = value_type->getSerialization(nested_settings);
    }
    else
    {
        key_serialization = key_type->getDefaultSerialization();
        value_serialization = value_type->getDefaultSerialization();
    }

    /// Don't use nested->getSerialization() to avoid creating exponentially growing number of serializations for deep nested maps.
    /// Instead, reuse already created serializations for keys and values.
    auto key_serialization_named = std::static_pointer_cast<const SerializationNamed>(SerializationNamed::create(key_serialization, "keys", SubstreamType::TupleElement));
    auto value_serialization_named = std::static_pointer_cast<const SerializationNamed>(SerializationNamed::create(value_serialization, "values", SubstreamType::TupleElement));
    auto nested_serialization = SerializationArray::create(SerializationTuple::create(SerializationTuple::ElementSerializations{key_serialization_named, value_serialization_named}, true));
    if (settings.map_serialization_version == MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS)
    {
        return SerializationMapWithKeyColumns::create(
            key_type, value_type, key_serialization, value_serialization, nested_serialization);
    }
    return SerializationMap::create(key_serialization, value_serialization, nested_serialization, settings.map_serialization_version);
}

bool DataTypeMap::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeMap & rhs_map = static_cast<const DataTypeMap &>(rhs);
    return nested->equals(*rhs_map.nested);
}

bool DataTypeMap::isValidKeyType(DataTypePtr key_type)
{
    return !isNullableOrLowCardinalityNullable(key_type);
}

DataTypePtr DataTypeMap::getNestedTypeWithUnnamedTuple() const
{
    const auto & from_array = assert_cast<const DataTypeArray &>(*nested);
    const auto & from_tuple = assert_cast<const DataTypeTuple &>(*from_array.getNestedType());
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(from_tuple.getElements()));
}

DataTypePtr DataTypeMap::getNestedDataType() const
{
    return assert_cast<const DataTypeArray &>(*nested).getNestedType();
}

void DataTypeMap::updateHashImpl(SipHash & hash) const
{
    key_type->updateHash(hash);
    value_type->updateHash(hash);
}

void DataTypeMap::forEachChild(const DB::IDataType::ChildCallback & callback) const
{
    callback(*key_type);
    callback(*value_type);
    key_type->forEachChild(callback);
    value_type->forEachChild(callback);
}

namespace
{

MutableColumnPtr parseMapSubcolumnKey(const DataTypePtr & key_type, std::string_view key_string, bool throw_if_null, const String & type_name, std::string_view subcolumn_name)
{
    auto key_column = key_type->createColumn();
    auto key_serialization = key_type->getDefaultSerialization();
    ReadBufferFromString buf(key_string);
    try
    {
        key_serialization->deserializeWholeText(*key_column, buf, FormatSettings{});
    }
    catch (...)
    {
        if (throw_if_null)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} doesn't have subcolumn {}", type_name, subcolumn_name);
        return nullptr;
    }
    return key_column;
}

ColumnPtr makePresenceColumnFromMap(const ColumnMap & column_map, const IColumn & key_column)
{
    auto result = ColumnUInt8::create();
    auto & data = result->getData();
    const size_t rows = column_map.size();
    data.resize_fill(rows, 0);

    const auto & keys_column = column_map.getNestedData().getColumn(0);
    const auto & offsets = column_map.getNestedColumn().getOffsets();
    for (size_t row = 0; row < rows; ++row)
    {
        const size_t begin = offsets[static_cast<ssize_t>(row) - 1];
        const size_t end = offsets[row];
        for (size_t pos = begin; pos < end; ++pos)
        {
            if (keys_column.compareAt(pos, 0, key_column, 0) == 0)
            {
                data[row] = 1;
                break;
            }
        }
    }
    return result;
}

}

/// Resolves a dynamic subcolumn like `map['key']` / `map.exists_key` by parsing the key
/// from the subcolumn name. Per-key serialization reads only that key's value or presence
/// stream; `basic` / `with_buckets` keep using `SerializationMapKeyValue`.
///
/// The returned path identifies the substream. `basic` / `with_buckets` use `MapKeyValue`
/// so `m['key']` still rewrites to the key subcolumn. `with_key_columns` uses `MapKey`
/// or `MapKeyPresence`, which name the per-key streams.
std::unique_ptr<IDataType::SubcolumnInfo> DataTypeMap::getDynamicSubcolumnInfo(std::string_view subcolumn_name, const SubstreamData & data, size_t /*initial_array_level*/, bool throw_if_null) const
{
    /// Must run before the `key_` prefix check: `keys_presence` starts with `key_`.
    /// Always expose the type so `hasSubcolumn` / sample blocks resolve it even when
    /// the current serialization is not `with_key_columns` (the part's info is used later).
    if (subcolumn_name == KEYS_PRESENCE_SUBCOLUMN)
    {
        auto res = std::make_unique<SubcolumnInfo>();
        res->data = SubstreamData(data.serialization);
        res->data.type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>());
        auto serialization = removeNamedSerialization(data.serialization);
        if (typeid_cast<const SerializationMapWithKeyColumns *>(serialization.get()))
            res->data.serialization = SerializationMapKeyPresenceMerge::create(serialization);
        else
            res->data.serialization = res->data.type->getDefaultSerialization();

        if (data.column)
        {
            const auto & map = assert_cast<const ColumnMap &>(*data.column);
            const auto manifest = SerializationMapWithKeyColumns::collectManifestFromColumn(map);
            const auto & keys_column = map.getNestedData().getColumn(0);
            const auto & map_offsets = map.getNestedColumn().getOffsets();
            const size_t rows = map.size();
            const size_t key_count = manifest.keys.size();

            auto presence_nested = ColumnUInt8::create();
            auto offsets = ColumnArray::ColumnOffsets::create();
            auto & presence = presence_nested->getData();
            auto & offs = offsets->getData();
            presence.reserve(rows * key_count);
            offs.reserve(rows);

            std::vector<Field> keys;
            keys.reserve(key_count);
            for (const auto & entry : manifest.keys)
                keys.push_back(entry.key);

            for (size_t row = 0; row < rows; ++row)
            {
                const size_t begin = map_offsets[static_cast<ssize_t>(row) - 1];
                const size_t end = map_offsets[row];
                for (const auto & key : keys)
                {
                    UInt8 present = 0;
                    for (size_t pos = begin; pos < end; ++pos)
                    {
                        if (keys_column[pos] == key)
                        {
                            present = 1;
                            break;
                        }
                    }
                    presence.push_back(present);
                }
                offs.push_back(presence.size());
            }
            res->data.column = ColumnArray::create(std::move(presence_nested), std::move(offsets));
        }

        res->substreams_path.emplace_back(ISerialization::Substream::MapKeyPresence);
        res->substreams_path.back().name_of_substream = String(KEYS_PRESENCE_SUBCOLUMN);
        return res;
    }

    const bool is_exists = subcolumn_name.starts_with(EXISTS_SUBCOLUMN_PREFIX);
    const bool is_key = subcolumn_name.starts_with(KEY_SUBCOLUMN_PREFIX);
    if (!is_exists && !is_key)
    {
        if (throw_if_null)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} doesn't have subcolumn {}", getName(), subcolumn_name);
        return nullptr;
    }

    std::string_view key_string = subcolumn_name.substr(is_exists ? EXISTS_SUBCOLUMN_PREFIX.size() : KEY_SUBCOLUMN_PREFIX.size());
    auto key_column = parseMapSubcolumnKey(key_type, key_string, throw_if_null, getName(), subcolumn_name);
    if (!key_column)
        return nullptr;

    auto serialization = removeNamedSerialization(data.serialization);
    Field key = (*key_column)[0];
    const auto * key_columns = typeid_cast<const SerializationMapWithKeyColumns *>(serialization.get());

    /// `exists_` is a type-level subcolumn. Analysis uses the default (`BASIC`)
    /// serialization, so this branch must not depend on `WITH_KEY_COLUMNS`.
    if (is_exists)
    {
        SerializationPtr exists_serialization = key_columns
            ? SerializationMapKeyPresence::create(serialization, key)
            : DataTypeUInt8().getDefaultSerialization();
        auto res = std::make_unique<SubcolumnInfo>();
        res->data = SubstreamData(exists_serialization).withType(std::make_shared<DataTypeUInt8>());
        if (data.column)
            res->data.column = makePresenceColumnFromMap(assert_cast<const ColumnMap &>(*data.column), *key_column);
        res->substreams_path.emplace_back(ISerialization::Substream::MapKeyPresence);
        res->substreams_path.back().name_of_substream = String(key_string);
        return res;
    }

    if (key_columns)
    {
        auto res = std::make_unique<SubcolumnInfo>();
        res->data = SubstreamData(
            SerializationMapWithKeyColumnsValue::create(key_columns->getValueSerialization(), value_type, serialization, key))
            .withType(value_type);
        if (data.column)
        {
            auto value_column = value_type->createColumn();
            extractKeyValueFromMap(*assert_cast<const ColumnMap &>(*data.column).getNestedColumnPtr(), *key_column, *value_column, 0, data.column->size());
            res->data.column = std::move(value_column);
        }
        res->substreams_path.emplace_back(ISerialization::Substream::MapKey);
        res->substreams_path.back().name_of_substream = String(key_string);
        return res;
    }

    /// `basic` / `with_buckets`: read only the bucket that contains the requested key.
    const auto & map_serialization = assert_cast<const SerializationMap &>(*serialization);
    auto key_value_serialization = SerializationMapKeyValue::create(
        map_serialization.getValueSerialization(),
        map_serialization.getNestedSerialization(),
        map_serialization.getMapSerializationVersion(),
        key_column->getPtr(),
        nested);
    auto res = std::make_unique<SubcolumnInfo>();
    res->data = SubstreamData(key_value_serialization).withType(value_type);

    res->substreams_path.emplace_back(ISerialization::Substream::MapKeyValue);
    res->substreams_path.back().name_of_substream = subcolumn_name;

    if (data.column)
    {
        const auto & column_map = assert_cast<const ColumnMap &>(*data.column);
        auto value_column = value_type->createColumn();
        extractKeyValueFromMap(*column_map.getNestedColumnPtr(), *key_column->getPtr(), *value_column, 0, data.column->size());
        res->data.column = std::move(value_column);
    }

    return res;
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Map data type family must have two arguments: key and value types");

    DataTypes nested_types;
    nested_types.reserve(arguments->children.size());

    for (const ASTPtr & child : arguments->children)
        nested_types.emplace_back(DataTypeFactory::instance().get(child));

    return std::make_shared<DataTypeMap>(nested_types);
}


void registerDataTypeMap(DataTypeFactory & factory)
{
    factory.registerDataType("Map", create, DataTypeFactory::Case::Sensitive, Documentation{
            .description = R"DOCS_MD(
Data type `Map(K, V)` stores key-value pairs.

Unlike other databases, maps are not unique in ClickHouse, i.e. a map can contain two elements with the same key.
(The reason for that is that maps are internally implemented as `Array(Tuple(K, V))`.)

You can use syntax `m[k]` to obtain the value for key `k` in map `m`.
Also, `m[k]` scans the map, i.e. the runtime of the operation is linear in the size of the map.
In MergeTree, a constant key can skip that scan. See [Bucketed Map Serialization in MergeTree](#bucketed-map-serialization) and [Per-key Map Serialization in MergeTree](#per-key-map-serialization).

**Parameters**

- `K` — The type of the Map keys. Arbitrary type except [Nullable](/reference/data-types/nullable) and [LowCardinality](/reference/data-types/lowcardinality) nested with [Nullable](/reference/data-types/nullable) types.
- `V` — The type of the Map values. Arbitrary type.

**Examples**

Create a table with a column of type map:

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
```

To select `key2` values:

```sql title="Query"
SELECT m['key2'] FROM tab;
```

```text title="Response"
┌─arrayElement(m, 'key2')─┐
│                      10 │
│                      20 │
│                      30 │
└─────────────────────────┘
```

If the requested key `k` is not contained in the map, `m[k]` returns the value type's default value, e.g. `0` for integer types and `''` for string types.
To check whether a key exists in a map, you can use function [mapContains](/reference/functions/regular-functions/tuple-map-functions#mapContainsKey).

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':100}), ({});
SELECT m['key1'] FROM tab;
```

```text title="Response"
┌─arrayElement(m, 'key1')─┐
│                     100 │
│                       0 │
└─────────────────────────┘
```

## Converting Tuple to Map {#converting-tuple-to-map}

Values of type `Tuple()` can be cast to values of type `Map()` using function [CAST](/reference/functions/regular-functions/type-conversion-functions#CAST):

**Example**

```sql title="Query"
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

```text title="Response"
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

## Reading subcolumns of Map {#reading-subcolumns-of-map}

To avoid reading the entire map, you can use subcolumns `keys` and `values` in some cases.

**Example**

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE = Memory;
INSERT INTO tab VALUES (map('key1', 1, 'key2', 2, 'key3', 3));

SELECT m.keys FROM tab; --   same as mapKeys(m)
SELECT m.values FROM tab; -- same as mapValues(m)
```

```text title="Response"
┌─m.keys─────────────────┐
│ ['key1','key2','key3'] │
└────────────────────────┘

┌─m.values─┐
│ [1,2,3]  │
└──────────┘
```

## Bucketed Map Serialization in MergeTree {#bucketed-map-serialization}

By default, a `Map` column in MergeTree is stored as a single `Array(Tuple(K, V))` stream.
Reading a single key with `m['key']` requires scanning the entire column — every key-value pair for every row — even if only one key is needed.
For maps with many distinct keys this becomes a bottleneck.

Bucketed serialization (`with_buckets`) splits the key-value pairs into multiple independent substreams (buckets) by hashing the key.
When a query accesses `m['key']`, only the bucket that contains that key is read from disk, skipping all other buckets.

### Enabling Bucketed Serialization {#enabling-bucketed-serialization}

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

To avoid slowing down inserts, you can keep `basic` serialization for zero-level parts (created during `INSERT`) and only use `with_buckets` for merged parts:

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'basic',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

### How It Works {#how-it-works}

When a data part is written with `with_buckets` serialization:

1. The average number of keys per row is computed from the block statistics.
2. The number of buckets is determined by the configured strategy (see [Settings](#bucketed-map-settings)).
3. Each key-value pair is assigned to a bucket by hashing the key: `bucket = hash(key) % num_buckets`.
4. Each bucket is stored as an independent substream with its own keys, values, and offsets.
5. A `buckets_info` metadata stream records the bucket count and statistics.

When a query reads a specific key (`m['key']`), the optimizer rewrites the expression to a key subcolumn (`m.key_<serialized_key>`).
The serialization layer computes which bucket the requested key belongs to and reads only that single bucket from disk.

When the full map is read (e.g., `SELECT m`), all buckets are read and reassembled into the original map. This is slower than `basic` serialization due to the overhead of reading and merging multiple substreams.

<Note>
Since version 26.8, `with_buckets` serialization preserves the original key order: an additional `bucket_indexes` substream records which bucket every key-value pair was taken from, so the map is reassembled in the order it was written instead of in bucket order.

Parts written by earlier versions do not contain that substream. Their maps are still reassembled in bucket order, and the original key order cannot be restored for them because it was never stored on disk — rewriting such a part (by a merge or `OPTIMIZE FINAL`) freezes the bucket order it currently has instead of recovering the insertion order. With `basic` serialization, the key order from inserted maps has always been preserved.
</Note>

The bucket count can vary between parts. When parts with different bucket counts are merged, the new part's bucket count is recalculated from the merged statistics. Parts with `basic` and `with_buckets` serialization can coexist in the same table and are merged transparently.

### Settings {#bucketed-map-settings}

| Setting | Default | Description |
|---------|---------|-------------|
| `map_serialization_version` | `basic` | Serialization format for `Map` columns. `basic` stores as a single array stream. `with_buckets` splits keys into buckets for faster single-key reads. `with_key_columns` stores each distinct key in its own streams. See [Per-key Map Serialization in MergeTree](#per-key-map-serialization). |
| `map_serialization_version_for_zero_level_parts` | `basic` | Serialization format for zero-level parts (created by `INSERT`). Allows keeping `basic` for inserts to avoid write overhead, while merged parts use `with_buckets`. |
| `max_buckets_in_map` | `32` | Upper bound on the number of buckets. The actual count depends on `map_buckets_strategy`. The maximum allowed value is 256. |
| `map_buckets_strategy` | `sqrt` | Strategy for computing bucket count from average map size: `constant` — always use `max_buckets_in_map`; `sqrt` — use `round(coefficient * sqrt(avg_size))`; `linear` — use `round(coefficient * avg_size)`. Result is clamped to `[1, max_buckets_in_map]`. |
| `map_buckets_coefficient` | `1.0` | Multiplier for `sqrt` and `linear` strategies. Ignored when strategy is `constant`. |
| `map_buckets_min_avg_size` | `32` | Minimum average keys per row to enable bucketing. If the average is below this threshold, a single bucket is used regardless of other settings. Set to `0` to disable the threshold. |

### Performance Trade-offs {#performance-trade-offs}

The following table summarizes the performance impact of `with_buckets` compared to `basic` serialization at various map sizes (10 to 10,000 keys per row). The bucket count was determined by the `sqrt` strategy capped at 32. The exact numbers depend on key/value types, data distribution, and hardware.

| Operation | 10 keys | 100 keys | 1,000 keys | 10,000 keys | Notes |
|-----------|---------|----------|------------|-------------|-------|
| **Single key lookup** (`m['key']`) | 1.6–3.2x faster | 4.5–7.7x faster | 16–39x faster | 21–49x faster | Reads only one bucket instead of the entire column. |
| **5 key lookups** | ~1x | 1.5–3.1x faster | 2.9–8.3x faster | 4.5–6.7x faster | Each key reads its own bucket; some buckets may overlap. |
| **PREWHERE** (`SELECT m WHERE m['key'] = ...`) | 1.5–3.0x faster | 2.9–7.3x faster | 5.3–31x faster | 20–45x faster | PREWHERE filter reads only one bucket; full map read only for matching rows. Speedup depends on selectivity — fewer matching granules means less full-map I/O. |
| **Full map scan** (`SELECT m`) | ~2x slower | ~2x slower | ~2x slower | ~2x slower | Must read and reassemble all buckets. |
| **INSERT** | 1.5–2.5x slower | 1.5–2.5x slower | 1.5–2.5x slower | 1.5–2.5x slower | Overhead of hashing keys and writing to multiple substreams. |

### Recommendations {#recommendations}

- **Small maps (< 32 keys on average):** Keep `basic` serialization. The overhead of bucketing is not justified for small maps. The default `map_buckets_min_avg_size = 32` enforces this automatically.
- **Medium maps (32–100 keys):** Use `with_buckets` with `sqrt` strategy if queries frequently access individual keys. The speedup is 4–8x for single-key lookups.
- **Large maps (100+ keys):** Use `with_buckets`. Single-key lookups are 16–49x faster. Consider `map_serialization_version_for_zero_level_parts = 'basic'` to keep insert speed close to the baseline.
- **Full map scans dominate the workload:** Keep `basic`. Bucketed serialization adds ~2x overhead for full scans.
- **Mixed workload (some key lookups, some full scans):** Use `with_buckets` with zero-level parts set to `basic`. The `PREWHERE` optimization reads only the relevant bucket for the filter, then reads the full map only for matching rows, giving a significant net speedup.

### Alternative Approaches {#map-alternatives}

If bucketed `Map` serialization does not fit your use case, there are two alternative approaches for improving key-level access performance:

#### Using the JSON Data Type {#using-the-json-data-type}

The [JSON](/reference/data-types/newjson) data type stores each frequent path as a separate dynamic subcolumn. Paths that exceed the `max_dynamic_paths` limit go into a [shared data structure](/reference/data-types/newjson#shared-data-structure), which can use `advanced` serialization for optimized single-path reads. See the [blog post](https://clickhouse.com/blog/json-data-type-gets-even-better) for a detailed overview of the `advanced` serialization.

| Aspect             | `Map` with buckets                                                                             | `JSON`                                                                                                                                                           |
|--------------------|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Single key read    | Reads one bucket (may contain other keys). All key-value pairs in the bucket are deserialized. | Frequent paths are read directly from dynamic subcolumns. Infrequent paths go to shared data; with `advanced` serialization, only the exact path's data is read. |
| Value types        | All values share the same type `V`                                                             | Each path can have its own type. Paths without a type hint use `Dynamic`.                                                                                        |
| Skip index support | Works with some index types created on `mapKeys`/`mapValues`                                   | Skip indexes can only be created on specific path subcolumns, not on all paths/values at once.                                                                   |
| Full column read   | ~2x slower than `basic` due to bucket reassembly                                               | Overhead from `Dynamic` type encoding and path reconstruction.                                                                                                   |
| Storage overhead   | Minimal additional metadata                                                                    | Higher due to `Dynamic` type encoding, path name storage, and additional metadata in `advanced` serialization.                                                   |
| Schema flexibility | Fixed key and value types at table creation                                                    | Fully dynamic — keys and value types can vary per row. Typed path hints can be declared for known paths.                                                         |

Use `JSON` when different keys need different value types, when the set of keys varies significantly across rows, or when frequently accessed keys are known in advance and can be declared as typed paths for direct subcolumn access.

#### Manual Sharding into Multiple Map Columns {#manual-sharding-into-multiple-map-columns}

You can manually split a single `Map` into multiple columns by key hash at the application level:

```sql
CREATE TABLE tab (
    id UInt64,
    m0 Map(String, UInt64),
    m1 Map(String, UInt64),
    m2 Map(String, UInt64),
    m3 Map(String, UInt64)
) ENGINE = MergeTree ORDER BY id;
```

During insertion, route each key-value pair to the column `m{hash(key) % 4}`. During queries, read from the specific column: `m{hash('target_key') % 4}['target_key']`.

| Aspect | `Map` with buckets | Manual sharding |
|--------|-------------------|-----------------|
| Ease of use | Transparent — handled by the storage engine | Requires application-level routing logic for inserts and selects |
| Vertical merge | Not supported — all buckets belong to one column | Supported — each `Map` column is an independent column and can be merged vertically |
| Schema changes | Bucket count adapts automatically per part | Changing the number of shards requires rewriting data or adding new columns |
| Query syntax | `m['key']` works directly | Must compute the correct column: `m0['key']`, `m1['key']`, etc. |
| Bucket granularity | Per-part, adapts to data statistics | Fixed at table creation |

Manual sharding is beneficial when vertical merges are important for reducing memory usage during merges of tables with many columns, or when the number of shards must be fixed and controlled explicitly. For most use cases, automatic bucketed serialization is simpler and sufficient.

## Per-key Map Serialization in MergeTree {#per-key-map-serialization}

`with_key_columns` stores each distinct key in its own streams, plus one shared presence stream. A query that reads one key, such as `m['key']` or `mapContains`, opens only that key's files. Bytes read follow the selected key, including when the other keys in the row are much larger. `with_buckets` still reads every key that landed in the same bucket.

### Enabling per-key serialization {#enabling-per-key-serialization}

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;
```

The format is implemented for Wide parts. Writing a Compact part raises `NOT_IMPLEMENTED`. An insert below [min_bytes_for_wide_part](/reference/settings/merge-tree-settings/min-bytes#min_bytes_for_wide_part) is a Compact part (10 MiB by default on a self-managed build). Set `min_bytes_for_wide_part` and `min_rows_for_wide_part` to `0` when every part must use `with_key_columns`.

`map_serialization_version_for_zero_level_parts` can stay `basic` for inserts. Merged parts then use `with_key_columns`. Those merged parts still have to be Wide.

### Read results {#per-key-map-read-results}

`m['k']` matches `basic` and `with_buckets`: the first value written for `k` in that row.

Reading the whole map differs in two ways:

- Keys come back in key comparison order. For `String` keys that is lexicographical order. `basic` and `with_buckets` keep the insertion order of the pairs.
- A repeated key in one row is stored once, keeping the first value. `length` and `m.size0` count the distinct keys present in the row. `basic` and `with_buckets` keep every pair, so `length` counts repeated keys.

`map('k', 1, 'k', 2, 'x', 3)` is read back as `{'k':1,'x':3}` with `length` equal to 2. Under `basic` and `with_buckets` the same value is `{'k':1,'k':2,'x':3}` with `length` equal to 3. `m['k']` is `1` in all three.

`mapKeys` and `mapValues` follow this same key order. Compare full maps with `mapSort` when the order itself is not significant.

`map_max_key_columns` defaults to `0` (no limit). The writer does not apply it yet, and there is no fallback stream for extra keys. Nested `Map` values stay on `basic` serialization.

**See Also**

- [map()](/reference/functions/regular-functions/tuple-map-functions#map) function
- [CAST()](/reference/functions/regular-functions/type-conversion-functions#CAST) function
- [-Map combinator for Map datatype](/reference/functions/aggregate-functions/combinators#-map)

## Related content {#related-content}

- Blog: [Building an Observability Solution with ClickHouse - Part 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)
)DOCS_MD",
            .syntax = "Map(K, V)",
            .examples = {},
            .related = {"Array", "Tuple"},
        });
}

}
