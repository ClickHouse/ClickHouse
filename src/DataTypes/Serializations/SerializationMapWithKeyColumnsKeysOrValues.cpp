#include <DataTypes/Serializations/SerializationMapWithKeyColumnsKeysOrValues.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

struct DeserializeBinaryBulkStateMapWithKeyColumnsKeysOrValues : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr with_key_columns_state;
    /// Per-key `.exists_` states for `m.keys`. Kept separate from the shared map state so a
    /// keys read does not advance that state's value streams.
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> exists_states;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumnsKeysOrValues>(*this);
        new_state->with_key_columns_state = with_key_columns_state ? with_key_columns_state->clone() : nullptr;
        for (auto & exists_state : new_state->exists_states)
            exists_state = exists_state ? exists_state->clone() : nullptr;
        return new_state;
    }
};

void SerializationMapWithKeyColumnsKeysOrValues::throwNoSerialization()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for Map with_key_columns keys/values subcolumn");
}

SerializationMapWithKeyColumnsKeysOrValues::SerializationMapWithKeyColumnsKeysOrValues(const SerializationPtr & map_with_key_columns_serialization_, bool is_keys_)
    : map_with_key_columns_serialization(map_with_key_columns_serialization_)
    , is_keys(is_keys_)
{
}

SerializationPtr SerializationMapWithKeyColumnsKeysOrValues::create(const SerializationPtr & map_with_key_columns_serialization_, bool is_keys_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapWithKeyColumnsKeysOrValues(map_with_key_columns_serialization_, is_keys_));
}

void SerializationMapWithKeyColumnsKeysOrValues::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * keys_or_values_state = data.deserialize_state
        ? checkAndGetState<DeserializeBinaryBulkStateMapWithKeyColumnsKeysOrValues>(data.deserialize_state)
        : nullptr;
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);

    MapKeyManifest manifest;
    if (keys_or_values_state)
        manifest = key_columns.getManifestFromState(keys_or_values_state->with_key_columns_state);

    /// `m.keys` names the keys from the manifest and filters them with `.exists_`.
    /// `m.values` still needs each key's value stream.
    for (const auto & entry : manifest.keys)
    {
        const auto key_name = key_columns.keyToStreamName(entry.key);

        if (!is_keys)
        {
            settings.path.push_back(Substream::MapKey);
            settings.path.back().name_of_substream = key_name;
            auto value_data = SubstreamData(key_columns.getValueSerialization()).withType(key_columns.getValueType());
            key_columns.getValueSerialization()->enumerateStreams(settings, callback, value_data);
            settings.path.pop_back();
        }

        if (entry.presence_kind == MapKeyPresenceKind::Tracked)
        {
            settings.path.push_back(Substream::MapKeyExists);
            settings.path.back().name_of_substream = key_name;
            auto exists_data = SubstreamData(key_columns.getExistsSerialization()).withType(std::make_shared<DataTypeUInt8>());
            key_columns.getExistsSerialization()->enumerateStreams(settings, callback, exists_data);
            settings.path.pop_back();
        }
    }
}

void SerializationMapWithKeyColumnsKeysOrValues::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationMapWithKeyColumnsKeysOrValues");
}

void SerializationMapWithKeyColumnsKeysOrValues::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationMapWithKeyColumnsKeysOrValues");
}

void SerializationMapWithKeyColumnsKeysOrValues::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationMapWithKeyColumnsKeysOrValues");
}

void SerializationMapWithKeyColumnsKeysOrValues::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto keys_or_values_state = std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumnsKeysOrValues>();

    /// `m.values` shares the full per-key prefix, including value streams.
    if (!is_keys)
    {
        map_with_key_columns_serialization->deserializeBinaryBulkStatePrefix(settings, keys_or_values_state->with_key_columns_state, cache);
        state = std::move(keys_or_values_state);
        return;
    }

    /// `m.keys` loads the manifest and opens only `.exists_` streams.
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);
    settings.path.push_back(Substream::MapKeysInfo);
    MapKeyManifest manifest;
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        if (const auto * cached = dynamic_cast<const SerializationMapWithKeyColumns::DeserializeBinaryBulkStateMapWithKeyColumns *>(cached_state.get()))
            manifest = cached->manifest;
        keys_or_values_state->with_key_columns_state = cached_state;
    }
    else
    {
        if (!settings.map_key_columns_manifest)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Missing key_columns manifest for with_key_columns Map");

        manifest = *settings.map_key_columns_manifest;
        auto stored = std::make_shared<SerializationMapWithKeyColumns::DeserializeBinaryBulkStateMapWithKeyColumns>();
        stored->manifest = manifest;
        addToSubstreamsDeserializeStatesCache(cache, settings.path, stored);
        keys_or_values_state->with_key_columns_state = stored;
    }
    settings.path.pop_back();

    keys_or_values_state->exists_states.resize(manifest.keys.size());
    for (size_t i = 0; i < manifest.keys.size(); ++i)
    {
        const auto & entry = manifest.keys[i];
        if (entry.presence_kind == MapKeyPresenceKind::AlwaysPresent)
            continue;

        settings.path.push_back(Substream::MapKeyExists);
        settings.path.back().name_of_substream = key_columns.keyToStreamName(entry.key);
        key_columns.getExistsSerialization()->deserializeBinaryBulkStatePrefix(settings, keys_or_values_state->exists_states[i], cache);
        settings.path.pop_back();
    }

    state = std::move(keys_or_values_state);
}

void SerializationMapWithKeyColumnsKeysOrValues::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * keys_or_values_state = checkAndGetState<DeserializeBinaryBulkStateMapWithKeyColumnsKeysOrValues>(state);
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);
    const auto & manifest = key_columns.getManifestFromState(keys_or_values_state->with_key_columns_state);
    auto & result = assert_cast<ColumnArray &>(column);
    auto & result_data = result.getData();
    auto & result_offsets = result.getOffsets();

    if (manifest.keys.empty())
    {
        result_offsets.resize_fill(result_offsets.size() + limit, result_data.size());
        return;
    }

    if (is_keys)
    {
        const size_t key_count = manifest.keys.size();
        std::vector<ColumnPtr> key_holders(key_count);
        std::vector<ColumnPtr> exists_columns(key_count);
        std::optional<size_t> rows;

        for (size_t i = 0; i < key_count; ++i)
        {
            const auto & entry = manifest.keys[i];
            auto holder = key_columns.getKeyType()->createColumn();
            holder->insert(entry.key);
            key_holders[i] = std::move(holder);

            if (entry.presence_kind == MapKeyPresenceKind::AlwaysPresent)
                continue;

            auto exists_column = ColumnUInt8::create();
            settings.path.push_back(Substream::MapKeyExists);
            settings.path.back().name_of_substream = key_columns.keyToStreamName(entry.key);
            if (!insertDataFromSubstreamsCacheIfAny(cache, settings, *exists_column))
            {
                key_columns.getExistsSerialization()->deserializeBinaryBulkWithMultipleStreams(
                    *exists_column, limit, settings, keys_or_values_state->exists_states[i], cache);
                addColumnWithNumReadRowsToSubstreamsCache(
                    cache, settings.path, exists_column->getPtr(), exists_column->size());
            }
            settings.path.pop_back();

            if (!rows)
                rows = exists_column->size();
            else if (exists_column->size() != *rows)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Map key presence row count mismatch while reading keys: {} vs {}",
                    exists_column->size(), *rows);

            exists_columns[i] = std::move(exists_column);
        }

        /// `AlwaysPresent` keys have no `.exists_` stream. A manifest of only those keys
        /// uses the requested row count; writers emit `Tracked`, so a real part has a stream.
        if (!rows)
            rows = limit;

        result_offsets.reserve(result_offsets.size() + *rows);
        for (size_t row = 0; row < *rows; ++row)
        {
            for (size_t i = 0; i < key_count; ++i)
            {
                const bool present = manifest.keys[i].presence_kind == MapKeyPresenceKind::AlwaysPresent
                    || assert_cast<const ColumnUInt8 &>(*exists_columns[i]).getData()[row];
                if (present)
                    result_data.insertFrom(*key_holders[i], 0);
            }
            result_offsets.push_back(result_data.size());
        }
        return;
    }

    /// `m.values`: reconstruct the full map, then project the values. Presence and
    /// order match the full-column read.
    auto map_column = DataTypeMap(key_columns.getKeyType(), key_columns.getValueType()).createColumn();
    /// Share the per-key stream reads with sibling subcolumns through `cache`: each
    /// `.key_`/`.exists_` stream is materialized once and reused, so no stream is
    /// advanced twice (see the standard Array/Nullable pattern).
    map_with_key_columns_serialization->deserializeBinaryBulkWithMultipleStreams(
        *map_column, limit, settings, keys_or_values_state->with_key_columns_state, cache);

    const auto & nested_array = assert_cast<const ColumnMap &>(*map_column).getNestedColumn();
    const auto & nested_tuple = assert_cast<const ColumnMap &>(*map_column).getNestedData();
    const auto & src_data = nested_tuple.getColumn(is_keys ? 0 : 1);
    const auto & src_offsets = nested_array.getOffsets();

    const size_t prev_offset = result_offsets.empty() ? 0 : result_offsets.back();
    result_data.insertRangeFrom(src_data, 0, src_data.size());
    for (auto offset : src_offsets)
        result_offsets.push_back(prev_offset + offset);
}

}
