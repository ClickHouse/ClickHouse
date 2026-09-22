#include <DataTypes/Serializations/SerializationMapWithKeyColumnsSize.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>
#include <DataTypes/Serializations/SerializationMapPresence.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

struct DeserializeBinaryBulkStateMapWithKeyColumnsSize : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr with_key_columns_state;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumnsSize>(*this);
        new_state->with_key_columns_state = with_key_columns_state ? with_key_columns_state->clone() : nullptr;
        return new_state;
    }
};

void SerializationMapWithKeyColumnsSize::throwNoSerialization()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for Map with_key_columns size subcolumn");
}

SerializationMapWithKeyColumnsSize::SerializationMapWithKeyColumnsSize(const SerializationPtr & map_with_key_columns_serialization_)
    : map_with_key_columns_serialization(map_with_key_columns_serialization_)
{
}

SerializationPtr SerializationMapWithKeyColumnsSize::create(const SerializationPtr & map_with_key_columns_serialization_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapWithKeyColumnsSize(map_with_key_columns_serialization_));
}

void SerializationMapWithKeyColumnsSize::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData &) const
{
    settings.path.push_back(Substream::MapKeysInfo);
    callback(settings.path);
    settings.path.pop_back();

    settings.path.push_back(Substream::MapKeyPresence);
    callback(settings.path);
    settings.path.pop_back();
}

void SerializationMapWithKeyColumnsSize::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationMapWithKeyColumnsSize");
}

void SerializationMapWithKeyColumnsSize::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationMapWithKeyColumnsSize");
}

void SerializationMapWithKeyColumnsSize::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationMapWithKeyColumnsSize");
}

void SerializationMapWithKeyColumnsSize::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto size_state = std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumnsSize>();
    map_with_key_columns_serialization->deserializeBinaryBulkStatePrefix(settings, size_state->with_key_columns_state, cache);
    state = std::move(size_state);
}

void SerializationMapWithKeyColumnsSize::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * /*cache*/) const
{
    auto * size_state = checkAndGetState<DeserializeBinaryBulkStateMapWithKeyColumnsSize>(state);
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);
    const auto & manifest = key_columns.getManifestFromState(size_state->with_key_columns_state);
    auto & data = assert_cast<ColumnUInt64 &>(column).getData();

    if (manifest.keys.empty())
    {
        data.resize_fill(data.size() + limit, 0);
        return;
    }

    settings.path.push_back(Substream::MapKeyPresence);
    if (settings.seek_stream_to_current_mark_callback)
        settings.seek_stream_to_current_mark_callback(settings.path);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Missing stream for Map key presence");

    std::vector<std::vector<UInt8>> presence;
    MapKeyPresenceBlock::deserializeAll(*stream, limit, manifest.keys.size(), presence);

    const size_t rows = presence.front().size();
    data.reserve(data.size() + rows);
    for (size_t row = 0; row < rows; ++row)
    {
        UInt64 size = 0;
        for (const auto & key_presence : presence)
            size += key_presence[row];
        data.push_back(size);
    }
}

}
