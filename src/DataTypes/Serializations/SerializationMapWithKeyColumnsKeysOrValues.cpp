#include <DataTypes/Serializations/SerializationMapWithKeyColumnsKeysOrValues.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>
#include <DataTypes/Serializations/SerializationMapPresence.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

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

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumnsKeysOrValues>(*this);
        new_state->with_key_columns_state = with_key_columns_state ? with_key_columns_state->clone() : nullptr;
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

    settings.path.push_back(Substream::MapKeysInfo);
    callback(settings.path);
    settings.path.pop_back();

    MapKeyManifest manifest;
    if (keys_or_values_state)
        manifest = key_columns.getManifestFromState(keys_or_values_state->with_key_columns_state);

    if (!is_keys)
    {
        for (const auto & entry : manifest.keys)
        {
            settings.path.push_back(Substream::MapKey);
            settings.path.back().name_of_substream = key_columns.keyToStreamName(entry.key);
            auto value_data = SubstreamData(key_columns.getValueSerialization()).withType(key_columns.getValueType());
            key_columns.getValueSerialization()->enumerateStreams(settings, callback, value_data);
            settings.path.pop_back();
        }
    }

    if (!manifest.keys.empty() || !keys_or_values_state)
    {
        settings.path.push_back(Substream::MapKeyPresence);
        callback(settings.path);
        settings.path.pop_back();
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
    map_with_key_columns_serialization->deserializeBinaryBulkStatePrefix(settings, keys_or_values_state->with_key_columns_state, cache);
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
        settings.path.push_back(Substream::MapKeyPresence);
        if (settings.seek_stream_to_current_mark_callback)
            settings.seek_stream_to_current_mark_callback(settings.path);
        auto * stream = settings.getter(settings.path);
        settings.path.pop_back();

        if (!stream)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Missing stream for Map key presence");

        std::vector<std::vector<UInt8>> presence;
        MapKeyPresenceBlock::deserializeAll(*stream, limit, manifest.keys.size(), presence);

        std::vector<MutableColumnPtr> key_holders(manifest.keys.size());
        for (size_t i = 0; i < manifest.keys.size(); ++i)
        {
            key_holders[i] = key_columns.getKeyType()->createColumn();
            key_holders[i]->insert(manifest.keys[i].key);
        }

        const size_t rows = presence.front().size();
        result_data.reserve(result_data.size() + rows * manifest.keys.size());
        for (size_t row = 0; row < rows; ++row)
        {
            for (size_t i = 0; i < manifest.keys.size(); ++i)
            {
                if (presence[i][row])
                    result_data.insertFrom(*key_holders[i], 0);
            }
            result_offsets.push_back(result_data.size());
        }
        return;
    }

    auto map_column = DataTypeMap(key_columns.getKeyType(), key_columns.getValueType()).createColumn();
    map_with_key_columns_serialization->deserializeBinaryBulkWithMultipleStreams(
        *map_column, limit, settings, keys_or_values_state->with_key_columns_state, cache);

    const auto & nested_array = assert_cast<const ColumnMap &>(*map_column).getNestedColumn();
    const auto & nested_tuple = assert_cast<const ColumnMap &>(*map_column).getNestedData();
    const auto & src_data = nested_tuple.getColumn(1);
    const auto & src_offsets = nested_array.getOffsets();

    const size_t prev_offset = result_offsets.empty() ? 0 : result_offsets.back();
    result_data.insertRangeFrom(src_data, 0, src_data.size());
    for (auto offset : src_offsets)
        result_offsets.push_back(prev_offset + offset);
}

}
