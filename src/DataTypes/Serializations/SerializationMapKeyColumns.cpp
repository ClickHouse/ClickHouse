#include <DataTypes/Serializations/SerializationMapKeyColumns.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Core/Defines.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <limits>
#include <set>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

const SerializationMapKeyColumns & requireMapKeyColumns(const SerializationPtr & serialization)
{
    const auto * map = typeid_cast<const SerializationMapKeyColumns *>(serialization.get());
    if (!map)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Per-key Map subcolumns require with_key_columns Map serialization");
    return *map;
}

/// The UInt8 serialization behind `m.exists.<key>`. Unlike `SerializationNamed`,
/// it appends no substream element to the path: the key's presence lives directly
/// in the `m.exists.<key>` stream.
SerializationPtr createPresenceSerialization()
{
    return SerializationNumber<UInt8>::create();
}

void collectFirstSeenKeys(
    const ColumnMap & map,
    std::vector<String> & keys,
    std::set<String> & seen)
{
    const auto & nested_array = map.getNestedColumn();
    const auto & tuple = map.getNestedData();
    const auto & keys_column = assert_cast<const ColumnString &>(tuple.getColumn(0));
    const auto & offsets = nested_array.getOffsets();

    for (size_t row = 0; row < map.size(); ++row)
    {
        size_t start = offsets[ssize_t(row) - 1];
        size_t end = offsets[row];
        std::set<std::string_view> row_keys;
        for (size_t i = start; i < end; ++i)
        {
            const std::string_view key = keys_column.getDataAt(i);
            if (!row_keys.insert(key).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate keys are not supported by with_key_columns Map serialization");
            if (key.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty keys are not supported by with_key_columns Map serialization");
            if (seen.emplace(key).second)
                keys.emplace_back(key);
        }
    }
}

/// The entries of the Map rows `[start, end)` grouped by key: the entries of `keys[i]`
/// are `entries[key_begin[i]..key_begin[i + 1])`, in row order. Duplicate or empty keys
/// in a row are rejected. The writer fixes the key set before writing, so a key
/// outside `keys` (sorted) means it did not see every key of the part.
struct MapEntriesByKey
{
    struct Entry
    {
        size_t row;
        size_t position;
    };
    std::vector<Entry> entries;
    std::vector<size_t> key_begin;
};

MapEntriesByKey groupMapEntriesByKey(const ColumnMap & map, const std::vector<String> & keys, size_t start, size_t end)
{
    const auto & source_keys = assert_cast<const ColumnString &>(map.getNestedData().getColumn(0));
    const auto & offsets = map.getNestedColumn().getOffsets();
    const size_t first = offsets[ssize_t(start) - 1];
    const size_t last = offsets[ssize_t(end) - 1];

    std::vector<UInt32> key_of_entry(last - first);
    std::vector<size_t> last_row_of_key(keys.size(), std::numeric_limits<size_t>::max());
    MapEntriesByKey result;
    result.key_begin.assign(keys.size() + 1, 0);
    for (size_t row = start; row < end; ++row)
    {
        for (size_t pos = offsets[ssize_t(row) - 1]; pos < offsets[row]; ++pos)
        {
            const std::string_view key = source_keys.getDataAt(pos);
            if (key.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty keys are not supported by with_key_columns Map serialization");
            auto it = std::lower_bound(keys.begin(), keys.end(), key);
            if (it == keys.end() || *it != key)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A key outside the part's key set in a with_key_columns Map");
            const size_t key_index = it - keys.begin();
            if (last_row_of_key[key_index] == row)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate keys are not supported by with_key_columns Map serialization");
            last_row_of_key[key_index] = row;
            key_of_entry[pos - first] = static_cast<UInt32>(key_index);
            ++result.key_begin[key_index + 1];
        }
    }

    for (size_t i = 0; i < keys.size(); ++i)
        result.key_begin[i + 1] += result.key_begin[i];

    std::vector<size_t> next = result.key_begin;
    result.entries.resize(last - first);
    for (size_t row = start; row < end; ++row)
        for (size_t pos = offsets[ssize_t(row) - 1]; pos < offsets[row]; ++pos)
            result.entries[next[key_of_entry[pos - first]]++] = {row, pos};
    return result;
}

/// The name identifying a `with_key_columns` Map inside its column: the subcolumn
/// name of its substream path (empty for a top-level Map).
String mapKeyColumnsPathName(const ISerialization::SubstreamPath & path)
{
    return ISerialization::getSubcolumnNameForStream(path);
}

}

struct SerializationMapKeyColumns::SerializeState : public ISerialization::SerializeBinaryBulkState
{
    /// The part's key set (sorted by byte order), fixed by the writer before any of
    /// the column's streams was opened.
    std::vector<String> keys;
    /// Per-key states of the value serialization (`m.values.<key>`), keyed by the raw key.
    std::map<String, SerializeBinaryBulkStatePtr> key_states;
    /// Per-key states of the UInt8 presence serialization (`m.exists.<key>`).
    std::map<String, SerializeBinaryBulkStatePtr> presence_states;
};

struct SerializationMapKeyColumns::DeserializeState : public ISerialization::DeserializeBinaryBulkState
{
    /// Keys of the part, sorted by byte order (exactly as stored in the `m.keys` stream).
    std::vector<String> keys;
    /// Per-key states of the value serialization (`m.values.<key>`), keyed by the raw key.
    std::map<String, DeserializeBinaryBulkStatePtr> value_states;
    /// Per-key states of the UInt8 presence serialization (`m.exists.<key>`).
    std::map<String, DeserializeBinaryBulkStatePtr> presence_states;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto copy = std::make_shared<DeserializeState>(*this);
        for (auto & [key, value_state] : copy->value_states)
            value_state = value_state ? value_state->clone() : nullptr;
        for (auto & [key, presence_state] : copy->presence_states)
            presence_state = presence_state ? presence_state->clone() : nullptr;
        return copy;
    }
};

struct DeserializeBinaryBulkStateMapKeyColumn : public ISerialization::DeserializeBinaryBulkState
{
    bool missing = false;
    ISerialization::DeserializeBinaryBulkStatePtr value_state;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto copy = std::make_shared<DeserializeBinaryBulkStateMapKeyColumn>(*this);
        copy->value_state = value_state ? value_state->clone() : nullptr;
        return copy;
    }
};

UInt128 SerializationMapKeyColumns::getHash(
    const SerializationPtr & text_serialization_,
    const SerializationPtr & value_serialization_)
{
    SipHash hash;
    hash.update("MapPerKey");
    hash.update(text_serialization_->getHash());
    hash.update(value_serialization_->getHash());
    return hash.get128();
}

SerializationMapKeyColumns::SerializationMapKeyColumns(
    const DataTypePtr & key_type_,
    const DataTypePtr & value_type_,
    const SerializationPtr & key_serialization_,
    const SerializationPtr & value_serialization_,
    const SerializationPtr & text_serialization_)
    : SerializationWrapper(text_serialization_)
    , key_type(key_type_)
    , value_type(value_type_)
    , key_serialization(key_serialization_)
    , value_serialization(value_serialization_)
{
}

SerializationPtr SerializationMapKeyColumns::create(
    const DataTypePtr & key_type_,
    const DataTypePtr & value_type_,
    const SerializationPtr & key_serialization_,
    const SerializationPtr & value_serialization_,
    const SerializationPtr & text_serialization_)
{
    auto create_serialization = [&]
    {
        return new SerializationMapKeyColumns(
            key_type_,
            value_type_,
            key_serialization_,
            value_serialization_,
            text_serialization_);
    };

    /// The pool key is built from the hashes of the inner serializations, which only
    /// poolable serializations have (e.g. a Map with `JSON` keys has none).
    if (!text_serialization_->supportsPooling() || !value_serialization_->supportsPooling())
        return SerializationPtr(create_serialization());
    return ISerialization::pooled(getHash(text_serialization_, value_serialization_), create_serialization);
}

ColumnPtr SerializationMapKeyColumns::sortMapKeys(const ColumnPtr & column, const IDataType & type)
{
    if (typeid_cast<const DataTypeMap *>(&type))
    {
        const auto & map = assert_cast<const ColumnMap &>(*column);
        const auto & keys = assert_cast<const ColumnString &>(map.getNestedData().getColumn(0));
        const auto & offsets = map.getNestedColumn().getOffsets();
        IColumn::Permutation permutation(keys.size());
        bool sorted = true;
        for (size_t row = 0; row < offsets.size(); ++row)
        {
            const size_t begin = offsets[ssize_t(row) - 1];
            const size_t end = offsets[row];
            for (size_t i = begin; i < end; ++i)
                permutation[i] = i;
            std::sort(permutation.begin() + begin, permutation.begin() + end, [&](size_t lhs, size_t rhs)
            {
                return keys.getDataAt(lhs) < keys.getDataAt(rhs);
            });
            for (size_t i = begin; sorted && i < end; ++i)
                sorted = permutation[i] == i;
        }
        if (sorted)
            return column;
        auto nested = ColumnArray::create(map.getNestedData().permute(permutation, 0), map.getNestedColumn().getOffsetsPtr());
        return ColumnMap::create(std::move(nested));
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(&type))
    {
        if (!hasMapsWithKeyColumns(*array_type->getNestedType()))
            return column;
        const auto & array = assert_cast<const ColumnArray &>(*column);
        auto data = sortMapKeys(array.getDataPtr(), *array_type->getNestedType());
        if (data == array.getDataPtr())
            return column;
        return ColumnArray::create(data, array.getOffsetsPtr());
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(&type))
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(*column);
        Columns elements;
        for (const auto & element : tuple.getColumns())
            elements.push_back(element);
        bool changed = false;
        for (size_t i = 0; i < elements.size(); ++i)
        {
            auto element = sortMapKeys(elements[i], *tuple_type->getElement(i));
            changed |= element != elements[i];
            elements[i] = std::move(element);
        }
        if (!changed)
            return column;
        return ColumnTuple::create(elements);
    }

    return column;
}

std::vector<String> SerializationMapKeyColumns::collectColumnKeys(const IColumn & column) const
{
    const auto & map = assert_cast<const ColumnMap &>(column);
    std::vector<String> keys;
    std::set<String> seen;
    collectFirstSeenKeys(map, keys, seen);
    return keys;
}

bool SerializationMapKeyColumns::hasMapsWithKeyColumns(const IDataType & type)
{
    if (typeid_cast<const DataTypeMap *>(&type))
        return true;
    if (const auto * array = typeid_cast<const DataTypeArray *>(&type))
        return hasMapsWithKeyColumns(*array->getNestedType());
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(&type))
        return std::ranges::any_of(tuple->getElements(), [](const auto & element) { return hasMapsWithKeyColumns(*element); });
    return false;
}

void SerializationMapKeyColumns::collectKeys(const IColumn & column, const SubstreamPath & path, MapKeyColumnsKeys & keys) const
{
    auto & path_keys = keys[mapKeyColumnsPathName(path)];
    std::set<String> seen(path_keys.begin(), path_keys.end());
    collectFirstSeenKeys(assert_cast<const ColumnMap &>(column), path_keys, seen);
}

const std::vector<String> & SerializationMapKeyColumns::getFixedKeys(const MapKeyColumnsKeys * keys, const SubstreamPath & path)
{
    static const std::vector<String> empty;
    if (!keys)
        return empty;
    auto it = keys->find(mapKeyColumnsPathName(path));
    return it == keys->end() ? empty : it->second;
}

void SerializationMapKeyColumns::enumeratePresenceStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data,
    const String & key,
    const DeserializeBinaryBulkStatePtr & presence_state) const
{
    auto presence_serialization = createPresenceSerialization();
    auto presence_data = SubstreamData(presence_serialization)
        .withType(std::make_shared<DataTypeUInt8>())
        .withColumn(data.column ? ColumnUInt8::create() : nullptr)
        .withDeserializeState(presence_state);

    settings.path.push_back(Substream::MapKeyPresence);
    settings.path.back().name_of_substream = key;
    settings.path.back().data = presence_data;
    presence_serialization->enumerateStreams(settings, callback, presence_data);
    settings.path.pop_back();
}

void SerializationMapKeyColumns::enumerateKeyStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data,
    const String & key,
    const DeserializeBinaryBulkStatePtr & value_state,
    const DeserializeBinaryBulkStatePtr & presence_state) const
{
    /// Value: `m.values.<key>`, delegating to the value serialization so that
    /// Nullable/LowCardinality substreams nest beneath. No column is attached:
    /// an empty Array value column would make the offsets substream look written
    /// (via `columns_substreams`) when only its prefix exists.
    settings.path.push_back(Substream::MapKeyValue);
    settings.path.back().name_of_substream = key;
    auto next_data = SubstreamData(value_serialization)
        .withType(value_type)
        .withDeserializeState(value_state);
    value_serialization->enumerateStreams(settings, callback, next_data);
    settings.path.pop_back();

    /// Presence: `m.exists.<key>` (UInt8).
    enumeratePresenceStreams(settings, callback, data, key, presence_state);
}

void SerializationMapKeyColumns::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    settings.path.push_back(Substream::MapKeys);
    settings.path.back().data = SubstreamData(data.serialization)
        .withType(std::make_shared<DataTypeString>())
        .withColumn(data.column ? ColumnString::create() : nullptr);
    callback(settings.path);
    settings.path.back().data = SubstreamData(data.serialization);
    settings.path.pop_back();

    const auto * deserialize_state = data.deserialize_state ? typeid_cast<const DeserializeState *>(data.deserialize_state.get()) : nullptr;
    if (deserialize_state)
    {
        for (const auto & key : deserialize_state->keys)
        {
            auto value_it = deserialize_state->value_states.find(key);
            auto presence_it = deserialize_state->presence_states.find(key);
            enumerateKeyStreams(
                settings,
                callback,
                data,
                key,
                value_it != deserialize_state->value_states.end() ? value_it->second : nullptr,
                presence_it != deserialize_state->presence_states.end() ? presence_it->second : nullptr);
        }
        return;
    }

    if (settings.collect_map_key_columns_keys && data.column)
        collectKeys(*data.column, settings.path, *settings.collect_map_key_columns_keys);

    /// On the write side the part's key set was fixed by the writer before any of the
    /// column's streams was opened; the streams exist even when the data column
    /// happens to contain none of the keys.
    if (settings.map_key_columns_keys)
    {
        for (const auto & key : getFixedKeys(settings.map_key_columns_keys, settings.path))
            enumerateKeyStreams(settings, callback, data, key, nullptr, nullptr);
        return;
    }

    if (data.column)
    {
        for (const auto & key : collectColumnKeys(*data.column))
            enumerateKeyStreams(settings, callback, data, key, nullptr, nullptr);
    }
}

void SerializationMapKeyColumns::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    /// The key set is fixed by the writer (see `MapKeyColumnsKeys`); direct serializer
    /// use adopts the keys of the prefix column, which also rejects duplicate and empty keys.
    auto map_state = std::make_shared<SerializeState>();
    if (settings.map_key_columns_keys)
        map_state->keys = getFixedKeys(settings.map_key_columns_keys, settings.path);
    else
        map_state->keys = collectColumnKeys(column);
    std::sort(map_state->keys.begin(), map_state->keys.end());
    for (const auto & key : map_state->keys)
    {
        map_state->key_states.emplace(key, SerializeBinaryBulkStatePtr{});
        map_state->presence_states.emplace(key, SerializeBinaryBulkStatePtr{});
    }
    state = map_state;

    settings.path.push_back(Substream::MapKeys);
    auto * keys_stream = settings.getter(settings.path);
    settings.path.pop_back();

    /// The manifest goes first, before the per-key prefixes. Compact parts run the state
    /// prefix for every granule, so the manifest is written per granule there; it is
    /// self-delimiting, so the reader consumes exactly its bytes.
    if (keys_stream)
        writeMapKeys(*keys_stream, map_state->keys);

    /// All per-key prefixes follow the manifest: readers read them in this order.
    auto presence_serialization = createPresenceSerialization();
    for (const auto & key : map_state->keys)
    {
        settings.path.push_back(Substream::MapKeyValue);
        settings.path.back().name_of_substream = key;
        value_serialization->serializeBinaryBulkStatePrefix(*value_type->createColumn(), settings, map_state->key_states.at(key));
        settings.path.pop_back();

        settings.path.push_back(Substream::MapKeyPresence);
        settings.path.back().name_of_substream = key;
        presence_serialization->serializeBinaryBulkStatePrefix(*ColumnUInt8::create(), settings, map_state->presence_states.at(key));
        settings.path.pop_back();
    }

    /// A null stream getter (e.g. a rejection probe that must not write anything)
    /// has no `m.keys` stream; the key extraction above still ran, so invalid keys
    /// are rejected even then.
    if (!keys_stream && !map_state->keys.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Map keys");
}

void SerializationMapKeyColumns::writeMapKeys(WriteBuffer & out, const std::vector<String> & keys) const
{
    /// The manifest is self-delimiting: a `varint` key count followed by the keys in
    /// `String` binary form. The count lets the reader stop exactly at the end of the
    /// manifest, which matters when the Map is nested inside an Array/Tuple and the
    /// outer serialization keeps writing granule data into the same nested stream.
    writeVarUInt(keys.size(), out);
    for (const auto & key : keys)
        writeStringBinary(key, out);
}

std::vector<String> SerializationMapKeyColumns::readMapKeys(ReadBuffer & in)
{
    std::vector<String> keys;
    UInt64 key_count = 0;
    readVarUInt(key_count, in);
    /// The count comes from disk: do not trust it for the allocation.
    keys.reserve(std::min<UInt64>(key_count, DEFAULT_NATIVE_BINARY_MAX_NUM_COLUMNS));
    /// A truncated stream surfaces as an exception from `readStrict` inside.
    for (UInt64 i = 0; i < key_count; ++i)
    {
        String key;
        readStringBinary(key, in);
        if (key.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Empty key in a with_key_columns Map keys stream");
        if (!keys.empty() && keys.back() >= key)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Keys in a with_key_columns Map keys stream are not strictly increasing at position {}",
                keys.size());
        keys.push_back(std::move(key));
    }
    return keys;
}

void SerializationMapKeyColumns::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * map_state = checkAndGetState<SerializeState>(state);

    /// Serializations without a state (e.g. numbers) need no suffix.
    for (auto & [key, key_state] : map_state->key_states)
    {
        if (!key_state)
            continue;
        settings.path.push_back(Substream::MapKeyValue);
        settings.path.back().name_of_substream = key;
        value_serialization->serializeBinaryBulkStateSuffix(settings, key_state);
        settings.path.pop_back();
    }

    auto presence_serialization = createPresenceSerialization();
    for (auto & [key, presence_state] : map_state->presence_states)
    {
        if (!presence_state)
            continue;
        settings.path.push_back(Substream::MapKeyPresence);
        settings.path.back().name_of_substream = key;
        presence_serialization->serializeBinaryBulkStateSuffix(settings, presence_state);
        settings.path.pop_back();
    }
}

void SerializationMapKeyColumns::readMapKeysStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    if (state)
        return;

    auto map_state = std::make_shared<DeserializeState>();
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        /// The cache is shared with other readers of the same column (e.g. the
        /// whole-Map tuple read for `m['a']` and the per-key subcolumn
        /// serializations for `m.key_a` in the same query) and is keyed by
        /// substream path only, so the cached state can have a different
        /// concrete type. Only reuse it when it really is a with_key_columns
        /// Map state; otherwise build a fresh one below.
        if (typeid_cast<const DeserializeState *>(cached_state.get()))
        {
            map_state = std::static_pointer_cast<DeserializeState>(cached_state->clone());
            state = std::move(map_state);
            return;
        }
    }

    settings.path.push_back(Substream::MapKeys);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();
    if (!stream)
    {
        /// The column is absent from this part (e.g. added by ALTER after the part was
        /// written), so there is no keys stream. Read it as an empty key set; the
        /// reader decides how to fill a missing whole column. A null keys stream
        /// means the column is not in this part's checksums; a keys stream that is
        /// present but references a missing key data file is still rejected as
        /// corruption by the reader.
    }
    else
    {
        map_state->keys = readMapKeys(*stream);
    }

    /// Share the manifest with the other readers of this Map. Only the keys are cached:
    /// the per-key prefix states are read by each reader from its own streams.
    auto cached_state = std::make_shared<DeserializeState>();
    cached_state->keys = map_state->keys;
    addToSubstreamsDeserializeStatesCache(cache, settings.path, std::move(cached_state));
    state = std::move(map_state);
}

void SerializationMapKeyColumns::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    readMapKeysStatePrefix(settings, state, cache);
    auto * map_state = checkAndGetState<DeserializeState>(state);
    auto presence_serialization = createPresenceSerialization();

    for (const auto & key : map_state->keys)
    {
        if (!map_state->value_states.contains(key))
        {
            settings.path.push_back(Substream::MapKeyValue);
            settings.path.back().name_of_substream = key;
            DeserializeBinaryBulkStatePtr value_state;
            value_serialization->deserializeBinaryBulkStatePrefix(settings, value_state, cache);
            settings.path.pop_back();
            map_state->value_states.emplace(key, std::move(value_state));
        }

        if (!map_state->presence_states.contains(key))
        {
            settings.path.push_back(Substream::MapKeyPresence);
            settings.path.back().name_of_substream = key;
            DeserializeBinaryBulkStatePtr presence_state;
            presence_serialization->deserializeBinaryBulkStatePrefix(settings, presence_state, cache);
            settings.path.pop_back();
            map_state->presence_states.emplace(key, std::move(presence_state));
        }
    }
}

void SerializationMapKeyColumns::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * map_state = checkAndGetState<SerializeState>(state);
    const auto & map = assert_cast<const ColumnMap &>(column);

    /// `limit == 0` follows the generic ISerialization contract ("write until the end of the column").
    const size_t end = limit && offset + limit < map.size() ? offset + limit : map.size();
    const size_t rows = offset < end ? end - offset : 0;

    /// One key's dense value and presence columns at a time: materializing every key at
    /// once would take `rows * keys` cells regardless of how sparse the Maps are.
    const auto by_key = groupMapEntriesByKey(map, map_state->keys, offset, offset + rows);
    const auto & source_values = map.getNestedData().getColumn(1);
    auto presence_serialization = createPresenceSerialization();
    for (size_t i = 0; i < map_state->keys.size(); ++i)
    {
        auto values = value_type->createColumn();
        auto presence = ColumnUInt8::create(rows, static_cast<UInt8>(0));
        values->reserve(rows);
        size_t next_row = offset;
        for (size_t e = by_key.key_begin[i]; e < by_key.key_begin[i + 1]; ++e)
        {
            const auto & entry = by_key.entries[e];
            values->insertManyDefaults(entry.row - next_row);
            values->insertFrom(source_values, entry.position);
            presence->getData()[entry.row - offset] = 1;
            next_row = entry.row + 1;
        }
        values->insertManyDefaults(offset + rows - next_row);

        const auto & key = map_state->keys[i];
        auto & key_state = map_state->key_states.at(key);
        settings.path.push_back(Substream::MapKeyValue);
        settings.path.back().name_of_substream = key;
        value_serialization->serializeBinaryBulkWithMultipleStreams(*values, 0, rows, settings, key_state);
        settings.path.pop_back();

        auto & presence_state = map_state->presence_states.at(key);
        settings.path.push_back(Substream::MapKeyPresence);
        settings.path.back().name_of_substream = key;
        presence_serialization->serializeBinaryBulkWithMultipleStreams(*presence, 0, rows, settings, presence_state);
        settings.path.pop_back();
    }
}

void SerializationMapKeyColumns::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * map_state = checkAndGetState<DeserializeState>(state);
    auto & map = assert_cast<ColumnMap &>(column);

    if (map_state->keys.empty())
    {
        /// The column is absent from this part (old part or added by a later ALTER):
        /// read it as empty Maps for every row.
        map.insertManyDefaults(limit);
        return;
    }

    /// Every key's presence and value streams hold exactly the part's rows (rows
    /// without the key have presence 0 and a default value). Read one key at a time and
    /// keep only its present values (key-major), so memory is bounded by the number of
    /// entries rather than `rows * keys`; then reorder them into rows.
    auto present_values = value_type->createColumn();
    std::vector<UInt32> present_rows;
    std::vector<UInt32> present_keys;
    size_t rows = 0;
    auto presence_serialization = createPresenceSerialization();

    for (size_t i = 0; i < map_state->keys.size(); ++i)
    {
        const auto & key = map_state->keys[i];
        auto values = value_type->createColumn();
        settings.path.push_back(Substream::MapKeyValue);
        settings.path.back().name_of_substream = key;
        value_serialization->deserializeBinaryBulkWithMultipleStreams(
            *values, limit, settings, map_state->value_states.at(key), cache);
        settings.path.pop_back();
        if (i && rows != values->size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Inconsistent row counts in with_key_columns Map value streams");
        rows = values->size();

        auto presence = ColumnUInt8::create();
        settings.path.push_back(Substream::MapKeyPresence);
        settings.path.back().name_of_substream = key;
        presence_serialization->deserializeBinaryBulkWithMultipleStreams(
            *presence, limit, settings, map_state->presence_states.at(key), cache);
        settings.path.pop_back();
        if (rows != presence->size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Inconsistent row counts in with_key_columns Map presence streams");

        const auto & presence_data = presence->getData();
        for (size_t row = 0; row < rows; ++row)
        {
            if (!presence_data[row])
                continue;
            present_values->insertFrom(*values, row);
            present_rows.push_back(static_cast<UInt32>(row));
            present_keys.push_back(static_cast<UInt32>(i));
        }
    }

    /// Counting sort by row; within a row, entries keep the part's sorted key order.
    /// A genuine SQL NULL value (V is Nullable) is copied as-is; only `presence == 0`
    /// means the row lacks the key.
    std::vector<size_t> row_begin(rows + 1, 0);
    for (auto row : present_rows)
        ++row_begin[row + 1];
    for (size_t row = 0; row < rows; ++row)
        row_begin[row + 1] += row_begin[row];
    IColumn::Permutation permutation(present_rows.size());
    {
        std::vector<size_t> next(row_begin.begin(), row_begin.end() - 1);
        for (size_t e = 0; e < present_rows.size(); ++e)
            permutation[next[present_rows[e]]++] = e;
    }

    auto & nested_array = map.getNestedColumn();
    auto & tuple = map.getNestedData();
    auto & keys_column = tuple.getColumn(0);
    auto & values_column = tuple.getColumn(1);
    auto & offsets = nested_array.getOffsets();

    const size_t base = keys_column.size();
    for (size_t e : permutation)
    {
        const auto & key = map_state->keys[present_keys[e]];
        keys_column.insertData(key.data(), key.size());
    }
    values_column.insertRangeFrom(*present_values->permute(permutation, 0), 0, permutation.size());
    for (size_t row = 0; row < rows; ++row)
        offsets.push_back(base + row_begin[row + 1]);
}

SerializationMapKeyColumn::SerializationMapKeyColumn(
    const SerializationPtr & value_serialization_,
    const SerializationPtr & map_serialization_,
    String key_,
    String key_subcolumn_name_)
    : SerializationWrapper(value_serialization_)
    , map_serialization(map_serialization_)
    , key(std::move(key_))
    , key_subcolumn_name(std::move(key_subcolumn_name_))
{
}

SerializationPtr SerializationMapKeyColumn::create(
    const SerializationPtr & value_serialization_,
    const SerializationPtr & map_serialization_,
    String key_,
    String key_subcolumn_name_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapKeyColumn(
        value_serialization_,
        map_serialization_,
        std::move(key_),
        std::move(key_subcolumn_name_)));
}

void SerializationMapKeyColumn::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * per_key = typeid_cast<const SerializationMapKeyColumns *>(map_serialization.get());
    if (!per_key)
    {
        /// The subcolumn was resolved against a serialization that is not the
        /// per-key one (e.g. when the block-size predictor enumerates the
        /// streams of a part that has no SerializationInfo). Only stream
        /// enumeration is needed there; fall back to enumerating the key's
        /// value stream by name without requiring the per-key serialization.
        settings.path.push_back(Substream::MapKeyValue);
        settings.path.back().name_of_substream = key;
        callback(settings.path);
        settings.path.pop_back();
        return;
    }

    settings.path.push_back(Substream::MapKeys);
    settings.path.back().data = SubstreamData(per_key->getPtr())
        .withType(std::make_shared<DataTypeString>())
        .withColumn(data.column ? ColumnString::create() : nullptr);
    callback(settings.path);
    settings.path.back().data = SubstreamData(per_key->getPtr());
    settings.path.pop_back();

    /// After prefix, a missing key has no data files. Do not list them: prefetch
    /// would try to open streams that were never written.
    const auto * value_state = data.deserialize_state
        ? typeid_cast<const DeserializeBinaryBulkStateMapKeyColumn *>(data.deserialize_state.get())
        : nullptr;
    if (value_state && value_state->missing)
        return;

    settings.path.push_back(Substream::MapKeyValue);
    settings.path.back().name_of_substream = key;
    auto next_data = SubstreamData(nested_serialization)
        .withType(data.type)
        .withColumn(data.column)
        .withSerializationInfo(data.serialization_info)
        .withDeserializeState(value_state ? value_state->value_state : data.deserialize_state);
    nested_serialization->enumerateStreams(settings, callback, next_data);
    settings.path.pop_back();
}

void SerializationMapKeyColumn::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto value_state = std::make_shared<DeserializeBinaryBulkStateMapKeyColumn>();
    const auto & per_key = requireMapKeyColumns(map_serialization);
    ISerialization::DeserializeBinaryBulkStatePtr map_state;
    per_key.readMapKeysStatePrefix(settings, map_state, cache);
    const auto & keys = checkAndGetState<SerializationMapKeyColumns::DeserializeState>(map_state)->keys;
    if (!std::binary_search(keys.begin(), keys.end(), key))
    {
        /// The reader leaves absent whole columns to DEFAULT evaluation.
        /// A standalone serializer without a keys stream can only return type defaults.
        value_state->missing = true;
        state = std::move(value_state);
        return;
    }

    settings.path.push_back(Substream::MapKeyValue);
    settings.path.back().name_of_substream = key;
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, value_state->value_state, cache);
    settings.path.pop_back();
    state = std::move(value_state);
}

void SerializationMapKeyColumn::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * value_state = checkAndGetState<DeserializeBinaryBulkStateMapKeyColumn>(state);
    if (value_state->missing)
    {
        column.insertManyDefaults(limit);
        return;
    }

    settings.path.push_back(Substream::MapKeyValue);
    settings.path.back().name_of_substream = key;
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(column, limit, settings, value_state->value_state, cache);
    settings.path.pop_back();
}

void SerializationMapKeyColumn::serializeBinaryBulkStatePrefix(
    const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    requireMapKeyColumns(map_serialization);
    settings.path.push_back(Substream::MapKeyValue);
    settings.path.back().name_of_substream = key;
    nested_serialization->serializeBinaryBulkStatePrefix(column, settings, state);
    settings.path.pop_back();
}

void SerializationMapKeyColumn::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::MapKeyValue);
    settings.path.back().name_of_substream = key;
    nested_serialization->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
    settings.path.pop_back();
}

void SerializationMapKeyColumn::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::MapKeyValue);
    settings.path.back().name_of_substream = key;
    nested_serialization->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}

namespace
{
struct MapPresenceState : public ISerialization::DeserializeBinaryBulkState
{
    std::vector<String> keys;
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> states;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto copy = std::make_shared<MapPresenceState>(*this);
        for (auto & state : copy->states)
            state = state ? state->clone() : nullptr;
        return copy;
    }
};
}

void SerializationMapKeyColumnsValues::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    /// `data.column` is the `Array(V)` subcolumn, not the Map: do not pass it on.
    nested_serialization->enumerateStreams(
        settings, callback, SubstreamData(nested_serialization).withDeserializeState(data.deserialize_state));
}

void SerializationMapKeyColumnsValues::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    const auto & map_serialization = requireMapKeyColumns(nested_serialization);
    auto map_column = DataTypeMap(map_serialization.getKeyType(), map_serialization.getValueType()).createColumn();
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(*map_column, limit, settings, state, cache);
    const auto & map = assert_cast<const ColumnMap &>(*map_column);
    auto values = ColumnArray::create(map.getNestedData().getColumnPtr(1), map.getNestedColumn().getOffsetsPtr());
    column.insertRangeFrom(*values, 0, values->size());
}

SerializationMapKeyPresence::SerializationMapKeyPresence(SerializationPtr map_serialization_)
    : SerializationWrapper(map_serialization_)
    , map_serialization(std::move(map_serialization_))
{
}

void SerializationMapKeyPresence::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    const auto * map = typeid_cast<const SerializationMapKeyColumns *>(map_serialization.get());
    if (!map)
        return;

    settings.path.push_back(Substream::MapKeys);
    settings.path.back().data = SubstreamData(map->getPtr())
        .withType(std::make_shared<DataTypeString>())
        .withColumn(data.column ? ColumnString::create() : nullptr);
    callback(settings.path);
    settings.path.back().data = SubstreamData(map->getPtr());
    settings.path.pop_back();

    const auto * state = data.deserialize_state ? typeid_cast<const MapPresenceState *>(data.deserialize_state.get()) : nullptr;
    std::vector<String> keys;
    if (state)
        keys = state->keys;
    for (size_t i = 0; i < keys.size(); ++i)
        map->enumeratePresenceStreams(settings, callback, data, keys[i], state ? state->states[i] : nullptr);
}

void SerializationMapKeyPresence::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    const auto & map = requireMapKeyColumns(map_serialization);
    ISerialization::DeserializeBinaryBulkStatePtr map_state;
    map.readMapKeysStatePrefix(settings, map_state, cache);

    auto presence_serialization = createPresenceSerialization();
    auto result = std::make_shared<MapPresenceState>();
    for (const auto & key : checkAndGetState<SerializationMapKeyColumns::DeserializeState>(map_state)->keys)
    {
        settings.path.push_back(Substream::MapKeyPresence);
        settings.path.back().name_of_substream = key;
        ISerialization::DeserializeBinaryBulkStatePtr presence_state;
        presence_serialization->deserializeBinaryBulkStatePrefix(settings, presence_state, cache);
        settings.path.pop_back();
        result->keys.push_back(key);
        result->states.push_back(std::move(presence_state));
    }
    state = std::move(result);
}

void SerializationMapKeyPresence::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * presence = checkAndGetState<MapPresenceState>(state);
    if (presence->keys.empty())
    {
        /// The part has no keys (or the whole Map column is absent): an empty Array per row.
        column.insertManyDefaults(limit);
        return;
    }
    /// Reads only the presence streams; value streams are never opened. Keeps only the present
    /// rows of each key, so memory is bounded by the number of entries rather than `rows * keys`.
    std::vector<UInt32> present_rows;
    std::vector<UInt32> present_keys;
    size_t rows = 0;
    auto serialization = createPresenceSerialization();
    for (size_t i = 0; i < presence->keys.size(); ++i)
    {
        auto values = ColumnUInt8::create();
        settings.path.push_back(Substream::MapKeyPresence);
        settings.path.back().name_of_substream = presence->keys[i];
        serialization->deserializeBinaryBulkWithMultipleStreams(*values, limit, settings, presence->states[i], cache);
        settings.path.pop_back();
        if (i && rows != values->size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Inconsistent row counts in with_key_columns Map presence streams");
        rows = values->size();
        const auto & data = values->getData();
        for (size_t row = 0; row < rows; ++row)
        {
            if (!data[row])
                continue;
            present_rows.push_back(static_cast<UInt32>(row));
            present_keys.push_back(static_cast<UInt32>(i));
        }
    }

    /// Counting sort by row; within a row, keys keep the part's sorted order.
    std::vector<size_t> row_begin(rows + 1, 0);
    for (auto row : present_rows)
        ++row_begin[row + 1];
    for (size_t row = 0; row < rows; ++row)
        row_begin[row + 1] += row_begin[row];
    std::vector<UInt32> row_keys(present_rows.size());
    {
        std::vector<size_t> next(row_begin.begin(), row_begin.end() - 1);
        for (size_t e = 0; e < present_rows.size(); ++e)
            row_keys[next[present_rows[e]]++] = present_keys[e];
    }

    auto & array = assert_cast<ColumnArray &>(column);
    auto & keys = assert_cast<ColumnString &>(array.getData());
    for (size_t row = 0; row < rows; ++row)
    {
        for (size_t e = row_begin[row]; e < row_begin[row + 1]; ++e)
            keys.insertData(presence->keys[row_keys[e]].data(), presence->keys[row_keys[e]].size());
        array.getOffsets().push_back(keys.size());
    }
}

}
