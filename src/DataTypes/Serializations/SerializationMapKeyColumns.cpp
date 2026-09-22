#include <DataTypes/Serializations/SerializationMapKeyColumns.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <algorithm>
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

/// Extract values for every requested key in one pass over the Map rows.
/// `values[i]` receives the physical values for `keys[i]` on rows `[start, end)`;
/// rows without the key get presence 0 and the value column's default. Duplicate
/// or empty keys in a row are rejected.
void extractMapKeyColumnValuesAndPresences(
    const ColumnMap & map,
    const std::vector<String> & keys,
    const std::vector<IColumn *> & values,
    const std::vector<IColumn *> & presences,
    size_t start,
    size_t end)
{
    const auto & source_keys = assert_cast<const ColumnString &>(map.getNestedData().getColumn(0));
    const auto & source_values = map.getNestedData().getColumn(1);
    const auto & offsets = map.getNestedColumn().getOffsets();
    for (size_t row = start; row < end; ++row)
    {
        std::map<std::string_view, size_t> positions;
        for (size_t pos = offsets[ssize_t(row) - 1]; pos < offsets[row]; ++pos)
        {
            if (!positions.emplace(source_keys.getDataAt(pos), pos).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate keys are not supported by with_key_columns Map serialization");
            if (source_keys.getDataAt(pos).empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty keys are not supported by with_key_columns Map serialization");
        }
        for (size_t i = 0; i < keys.size(); ++i)
        {
            auto & presence = assert_cast<ColumnUInt8 &>(*presences[i]);
            auto it = positions.find(keys[i]);
            presence.getData().push_back(it != positions.end());
            if (it == positions.end())
                values[i]->insertDefault();
            else
                values[i]->insertFrom(source_values, it->second);
        }
    }
}

/// Serialize state carrying a pre-declared key set, handed from the writer
/// (which knows the part's keys before opening any of the column's streams) to
/// `serializeBinaryBulkStatePrefix`, so the prefixes of every key's streams are
/// written into streams that were opened before any mark exists.
struct SeedKeysState : public ISerialization::SerializeBinaryBulkState
{
    std::vector<String> keys;
};

}

struct SerializationMapKeyColumns::SerializeState : public ISerialization::SerializeBinaryBulkState
{
    /// The part's key set (sorted by byte order), fixed before the first block's
    /// data is written: seeded from the part-level key set precomputed by the
    /// writer, pre-seeded by `addPlannedKeys` for merges with a planned key union,
    /// or adopted from the whole first block for direct serializer use.
    std::vector<String> keys;
    std::set<String> registered_keys;
    /// Per-key states of the value serialization (`m.values.<key>`), keyed by the raw key.
    std::map<String, SerializeBinaryBulkStatePtr> key_states;
    /// Per-key states of the UInt8 presence serialization (`m.exists.<key>`).
    std::map<String, SerializeBinaryBulkStatePtr> presence_states;
    /// The block currently being written: per-key full-block value/presence columns,
    /// materialized once per block so that every granule writes a slice of them.
    std::map<String, ColumnPtr> key_value_columns;
    std::map<String, ColumnPtr> key_presence_columns;
    const IColumn * extraction_source = nullptr;
    bool suffix_written = false;
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
    return ISerialization::pooled(
        getHash(text_serialization_, value_serialization_),
        [&]
        {
            return new SerializationMapKeyColumns(
                key_type_,
                value_type_,
                key_serialization_,
                value_serialization_,
                text_serialization_);
        });
}

std::vector<String> SerializationMapKeyColumns::collectColumnKeys(const IColumn & column) const
{
    const auto & map = assert_cast<const ColumnMap &>(column);
    std::vector<String> keys;
    std::set<String> seen;
    collectFirstSeenKeys(map, keys, seen);
    return keys;
}

std::vector<String> SerializationMapKeyColumns::getMissingKeys(const IColumn & column, const SerializeBinaryBulkState & state) const
{
    const auto & map_state = typeid_cast<const SerializeState &>(state);

    std::vector<String> collected = collectColumnKeys(column);

    std::vector<String> missing_keys;
    missing_keys.reserve(collected.size());
    for (auto & key : collected)
    {
        if (!map_state.registered_keys.contains(key))
            missing_keys.push_back(std::move(key));
    }
    return missing_keys;
}

void SerializationMapKeyColumns::addPlannedKeys(SerializeBinaryBulkStatePtr & state, const std::vector<String> & keys) const
{
    auto * map_state = typeid_cast<SerializeState *>(state.get());
    if (!map_state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid serialize state for SerializationMapKeyColumns");
    if (map_state->extraction_source)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot register planned Map keys after block data was written");

    for (const auto & key : keys)
    {
        if (!map_state->registered_keys.insert(key).second)
            continue;
        map_state->keys.push_back(key);
        map_state->key_states.emplace(key, SerializeBinaryBulkStatePtr{});
        map_state->presence_states.emplace(key, SerializeBinaryBulkStatePtr{});
    }
    std::sort(map_state->keys.begin(), map_state->keys.end());
}

bool SerializationMapKeyColumns::hasRegisteredKeys(const SerializeBinaryBulkState & state) const
{
    return !typeid_cast<const SerializeState &>(state).registered_keys.empty();
}

ISerialization::SerializeBinaryBulkStatePtr SerializationMapKeyColumns::createSeedKeysState(std::vector<String> keys)
{
    auto state = std::make_shared<SeedKeysState>();
    state->keys = std::move(keys);
    return state;
}

std::vector<String> SerializationMapKeyColumns::getSeedKeys(const SerializeBinaryBulkState & state)
{
    if (const auto * seed = typeid_cast<const SeedKeysState *>(&state))
        return seed->keys;
    return {};
}

bool SerializationMapKeyColumns::hasExtractedKeys(const SerializeBinaryBulkState & state) const
{
    return typeid_cast<const SerializeState &>(state).extraction_source != nullptr;
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

    /// On the write side the part's key set is declared up front (the writer
    /// knows the part-level key set before opening any of the column's streams;
    /// a merge pre-seeds the planned union through `addPlannedKeys`): the streams
    /// exist even when the data column happens to contain none of the keys.
    if (data.serialize_state)
    {
        if (const auto * seed = typeid_cast<const SeedKeysState *>(data.serialize_state.get()))
        {
            for (const auto & key : seed->keys)
                enumerateKeyStreams(settings, callback, data, key, nullptr, nullptr);
            return;
        }
        if (const auto * serialize_state = typeid_cast<const SerializeState *>(data.serialize_state.get()))
        {
            for (const auto & key : serialize_state->keys)
                enumerateKeyStreams(settings, callback, data, key, nullptr, nullptr);
            return;
        }
    }

    if (data.column)
    {
        const auto & map = assert_cast<const ColumnMap &>(*data.column);
        std::vector<String> keys;
        std::set<String> seen;
        collectFirstSeenKeys(map, keys, seen);
        for (const auto & key : keys)
            enumerateKeyStreams(settings, callback, data, key, nullptr, nullptr);
    }
}

void SerializationMapKeyColumns::enumerateWriteStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data,
    const std::vector<String> & keys) const
{
    settings.path.push_back(Substream::MapKeys);
    settings.path.back().data = SubstreamData(data.serialization)
        .withType(std::make_shared<DataTypeString>())
        .withColumn(data.column ? ColumnString::create() : nullptr);
    callback(settings.path);
    settings.path.back().data = SubstreamData(data.serialization);
    settings.path.pop_back();

    for (const auto & key : keys)
        enumerateKeyStreams(settings, callback, data, key, nullptr, nullptr);
}

void SerializationMapKeyColumns::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto map_state_ptr = std::make_shared<SerializeState>();

    /// The key set is either pre-declared by the caller (the writer hands over
    /// the part-level key set as a `SeedKeysState` before opening any of the
    /// column's streams; a merge pre-registers the planned union in the state it
    /// replaces) or, for direct serializer use, adopted from the column handed to
    /// the prefix.
    std::vector<String> declared_keys;
    if (auto * seed = typeid_cast<SeedKeysState *>(state.get()))
        declared_keys = std::move(seed->keys);
    else if (state && hasRegisteredKeys(*state))
        map_state_ptr = std::static_pointer_cast<SerializeState>(std::move(state));
    else
        /// Direct serializer use (no pre-declared key set): adopt the prefix
        /// column's keys. This also rejects duplicate/empty keys.
        declared_keys = collectColumnKeys(column);

    state = map_state_ptr;
    if (!declared_keys.empty())
        addPlannedKeys(state, std::move(declared_keys));
    auto * map_state = checkAndGetState<SerializeState>(state);

    settings.path.push_back(Substream::MapKeys);
    auto * keys_stream = settings.getter(settings.path);
    settings.path.pop_back();

    /// The keys list goes first in the `m.keys` stream, before any of the per-key
    /// stream prefixes: in a compact part the manifest and the prefixes share the
    /// same compressed stream (same codec), so the manifest must be written while the
    /// stream is at the position the manifest's mark was recorded at — a prefix written
    /// in between would land in the manifest's compressed block.
    if (keys_stream && !map_state->extraction_source)
        writeMapKeys(*keys_stream, map_state->keys);

    auto presence_serialization = createPresenceSerialization();
    for (const auto & key : map_state->keys)
    {
        /// Registering the discovered keys only when the first block's data starts
        /// would open their per-key stream prefixes (e.g. the `LowCardinality`
        /// dictionary version) after the writer already recorded the first marks of
        /// the block, and the marks would point before the prefixes. Write all
        /// stream prefixes here, before the first granule, so every key's first
        /// mark points past its prefix.
        auto & key_state = map_state->key_states.at(key);
        settings.path.push_back(Substream::MapKeyValue);
        settings.path.back().name_of_substream = key;
        auto empty_values = value_type->createColumn();
        value_serialization->serializeBinaryBulkStatePrefix(*empty_values, settings, key_state);
        settings.path.pop_back();

        auto & presence_state = map_state->presence_states.at(key);
        settings.path.push_back(Substream::MapKeyPresence);
        settings.path.back().name_of_substream = key;
        auto empty_presence = ColumnUInt8::create();
        presence_serialization->serializeBinaryBulkStatePrefix(*empty_presence, settings, presence_state);
        settings.path.pop_back();
    }

    /// A null stream getter (e.g. a rejection probe that must not write anything)
    /// has no `m.keys` stream; the key extraction above still ran, so invalid keys
    /// are rejected even then.
    if (!keys_stream && !map_state->keys.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Map keys");
}

ColumnPtr SerializationMapKeyColumns::createMapKeysColumn(const std::vector<String> & keys) const
{
    auto column = ColumnString::create();
    column->reserve(keys.size());
    for (const auto & key : keys)
        column->insertData(key.data(), key.size());
    return column;
}

void SerializationMapKeyColumns::writeMapKeys(WriteBuffer & out, const std::vector<String> & keys) const
{
    /// The stream payload is a plain serialized `ColumnString`, exactly what
    /// `DataTypeString`'s default serialization writes.
    auto keys_column = createMapKeysColumn(keys);
    SerializationString::create()->serializeBinaryBulk(*keys_column, out, 0, keys.size());
}

std::vector<String> SerializationMapKeyColumns::readMapKeys(ReadBuffer & in) const
{
    std::vector<String> keys;
    /// The stream carries no row count, so read keys until end of stream. A truncated
    /// stream surfaces as an exception from `readStrict` inside.
    while (!in.eof())
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

    /// The per-key stream prefixes were written by `serializeBinaryBulkStatePrefix`
    /// (before the first mark was recorded) whenever the part's key set was known
    /// there. A null stream getter (a rejection probe) or an empty key set skips
    /// the prefixes; the suffixes of those states are skipped too.
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

    /// The `m.keys` payload was written by `serializeBinaryBulkStatePrefix` at the
    /// start of the stream; nothing is appended to it here.
    map_state->suffix_written = true;
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
        state = std::move(map_state);
        return;
    }

    map_state->keys = readMapKeys(*stream);
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

    /// The part's key set was fixed and all per-key stream prefixes were written by
    /// `serializeBinaryBulkStatePrefix`, before the writer recorded the first mark of
    /// the part (opening a prefix here would move the stream's start past the first
    /// mark). Later blocks of an insert must stay within the part-level key set
    /// precomputed by the writer; the caller rejects keys that violate it. Merges
    /// pre-seed the planned key union via `addPlannedKeys`.
    if (map_state->extraction_source != &column)
    {
        std::vector<IColumn *> value_ptrs;
        std::vector<IColumn *> presence_ptrs;
        value_ptrs.reserve(map_state->keys.size());
        presence_ptrs.reserve(map_state->keys.size());
        for (const auto & key : map_state->keys)
        {
            map_state->key_value_columns[key] = value_type->createColumn();
            map_state->key_presence_columns[key] = ColumnUInt8::create();
            value_ptrs.push_back(map_state->key_value_columns[key]->assumeMutable().get());
            presence_ptrs.push_back(map_state->key_presence_columns[key]->assumeMutable().get());
        }
        extractMapKeyColumnValuesAndPresences(map, map_state->keys, value_ptrs, presence_ptrs, 0, map.size());
        map_state->extraction_source = &column;
    }

    /// `limit == 0` follows the generic ISerialization contract ("write until the end of
    /// the column"); the wide writer always passes an explicit non-zero row count.
    size_t end = limit && offset + limit < map.size() ? offset + limit : map.size();
    size_t rows = end - offset;
    if (!rows)
        return;

    auto presence_serialization = createPresenceSerialization();

    for (const auto & key : map_state->keys)
    {
        auto & key_state = map_state->key_states.at(key);
        if (!key_state)
        {
            /// A null prefix state means the prefix could not write anything (e.g.
            /// the writer opened this key's streams only after the state prefix ran
            /// — the writer seeds the key set first precisely to avoid this).
            /// Open it now; in the wide writer this must not happen once marks were
            /// recorded, so a missing stream surfaces as an exception from the
            /// stream getter rather than a silently shifted mark.
            settings.path.push_back(Substream::MapKeyValue);
            settings.path.back().name_of_substream = key;
            value_serialization->serializeBinaryBulkStatePrefix(*map_state->key_value_columns.at(key), settings, key_state);
            settings.path.pop_back();
        }
        settings.path.push_back(Substream::MapKeyValue);
        settings.path.back().name_of_substream = key;
        value_serialization->serializeBinaryBulkWithMultipleStreams(*map_state->key_value_columns.at(key), offset, rows, settings, key_state);
        settings.path.pop_back();

        auto & presence_state = map_state->presence_states.at(key);
        if (!presence_state)
        {
            settings.path.push_back(Substream::MapKeyPresence);
            settings.path.back().name_of_substream = key;
            presence_serialization->serializeBinaryBulkStatePrefix(*map_state->key_presence_columns.at(key), settings, presence_state);
            settings.path.pop_back();
        }
        settings.path.push_back(Substream::MapKeyPresence);
        settings.path.back().name_of_substream = key;
        presence_serialization->serializeBinaryBulkWithMultipleStreams(*map_state->key_presence_columns.at(key), offset, rows, settings, presence_state);
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
    /// without the key have presence 0 and a default value), so each key's columns
    /// are read over the same range and filtered by presence afterwards.
    std::vector<ColumnPtr> value_columns;
    std::vector<ColumnPtr> presence_columns;
    value_columns.reserve(map_state->keys.size());
    presence_columns.reserve(map_state->keys.size());
    size_t rows = 0;
    auto presence_serialization = createPresenceSerialization();

    for (const auto & key : map_state->keys)
    {
        auto values = value_type->createColumn();
        settings.path.push_back(Substream::MapKeyValue);
        settings.path.back().name_of_substream = key;
        value_serialization->deserializeBinaryBulkWithMultipleStreams(
            *values, limit, settings, map_state->value_states.at(key), cache);
        settings.path.pop_back();
        if (!value_columns.empty() && rows != values->size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Inconsistent row counts in with_key_columns Map value streams");
        rows = values->size();
        value_columns.emplace_back(std::move(values));

        auto presence = ColumnUInt8::create();
        settings.path.push_back(Substream::MapKeyPresence);
        settings.path.back().name_of_substream = key;
        presence_serialization->deserializeBinaryBulkWithMultipleStreams(
            *presence, limit, settings, map_state->presence_states.at(key), cache);
        settings.path.pop_back();
        if (rows != presence->size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Inconsistent row counts in with_key_columns Map presence streams");
        presence_columns.emplace_back(std::move(presence));
    }

    auto & nested_array = map.getNestedColumn();
    auto & tuple = map.getNestedData();
    auto & keys_column = tuple.getColumn(0);
    auto & values_column = tuple.getColumn(1);
    auto & offsets = nested_array.getOffsets();

    for (size_t row = 0; row < rows; ++row)
    {
        /// Present keys go to the Map in the part's sorted key order. A genuine
        /// SQL NULL value (V is Nullable and the value at this row is NULL) is
        /// copied as-is; only `presence == 0` means the row lacks the key.
        for (size_t i = 0; i < map_state->keys.size(); ++i)
        {
            if (!assert_cast<const ColumnUInt8 &>(*presence_columns[i]).getData()[row])
                continue;
            keys_column.insertData(map_state->keys[i].data(), map_state->keys[i].size());
            values_column.insertFrom(*value_columns[i], row);
        }
        offsets.push_back(keys_column.size());
    }
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

SerializationMapKeyPresence::SerializationMapKeyPresence(SerializationPtr map_serialization_, std::optional<String> key_)
    : SerializationWrapper(map_serialization_)
    , map_serialization(std::move(map_serialization_))
    , requested_key(std::move(key_))
{
}

void SerializationMapKeyPresence::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    const auto * map = typeid_cast<const SerializationMapKeyColumns *>(map_serialization.get());
    if (!map)
    {
        /// The subcolumn was resolved against a serialization that is not the
        /// per-key one (e.g. when the block-size predictor enumerates the
        /// streams of a part that has no SerializationInfo, the existence
        /// subcolumn is built on the default whole-Map serialization). Only
        /// stream enumeration is needed there; fall back to enumerating the
        /// requested key's presence stream by name without requiring the
        /// per-key serialization.
        if (requested_key)
        {
            settings.path.push_back(Substream::MapKeyPresence);
            settings.path.back().name_of_substream = *requested_key;
            callback(settings.path);
            settings.path.pop_back();
        }
        return;
    }

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
    else if (requested_key)
        keys.push_back(*requested_key);
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
        if (requested_key && key != *requested_key)
            continue;
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
        /// The requested key is absent from this part (or the whole Map column is
        /// absent): `exists_<key>` is all zeros and `keys` is an empty Array per row.
        column.insertManyDefaults(limit);
        return;
    }
    /// Reads only the presence streams; value streams are never opened.
    std::vector<ColumnPtr> presence_columns;
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
        presence_columns.push_back(std::move(values));
    }
    if (requested_key)
    {
        column.insertRangeFrom(*presence_columns.front(), 0, rows);
        return;
    }
    auto & array = assert_cast<ColumnArray &>(column);
    auto & keys = assert_cast<ColumnString &>(array.getData());
    for (size_t row = 0; row < rows; ++row)
    {
        for (size_t i = 0; i < presence->keys.size(); ++i)
            if (assert_cast<const ColumnUInt8 &>(*presence_columns[i]).getData()[row])
                keys.insertData(presence->keys[i].data(), presence->keys[i].size());
        array.getOffsets().push_back(keys.size());
    }
}

}
