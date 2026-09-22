#include <DataTypes/Serializations/SerializationMapKeyPresenceMerge.h>
#include <DataTypes/Serializations/SerializationMapPresence.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

void SerializationMapKeyPresenceMerge::throwNoSerialization()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for Map key presence merge");
}

SerializationMapKeyPresenceMerge::SerializationMapKeyPresenceMerge(
    const SerializationPtr & map_with_key_columns_serialization_,
    MapKeyManifest write_manifest_,
    bool for_write_)
    : map_with_key_columns_serialization(map_with_key_columns_serialization_)
    , write_manifest(std::move(write_manifest_))
    , for_write(for_write_)
{
}

SerializationPtr SerializationMapKeyPresenceMerge::create(
    const SerializationPtr & map_with_key_columns_serialization_,
    MapKeyManifest write_manifest_,
    bool for_write_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapKeyPresenceMerge(
        map_with_key_columns_serialization_, std::move(write_manifest_), for_write_));
}

void SerializationMapKeyPresenceMerge::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData &) const
{
    settings.path.push_back(Substream::MapKeysInfo);
    callback(settings.path);
    settings.path.pop_back();

    const bool announce_presence = for_write ? !write_manifest.keys.empty() : true;
    if (!announce_presence)
        return;

    settings.path.push_back(Substream::MapKeyPresence);
    if (!settings.check_stream_exists_callback || settings.check_stream_exists_callback(settings.path))
        callback(settings.path);
    settings.path.pop_back();
}

void SerializationMapKeyPresenceMerge::flushPendingPresence(
    SerializeBinaryBulkSettings & settings,
    SerializeState & state) const
{
    if (write_manifest.keys.empty())
    {
        state.pending_presence.clear();
        state.pending_rows = 0;
        return;
    }

    settings.path.push_back(Substream::MapKeyPresence);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Map key presence during merge write");

    if (state.pending_rows > 0)
        MapKeyPresenceBlock::serialize(*stream, state.pending_rows, state.pending_presence);

    state.pending_presence.clear();
    state.pending_rows = 0;
}

void SerializationMapKeyPresenceMerge::serializeBinaryBulkStatePrefix(
    const IColumn &,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);

    settings.path.push_back(Substream::MapKeysInfo);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Map keys info during presence merge write");

    SerializationMapWithKeyColumns::writeManifest(*stream, key_columns.getKeySerialization(), write_manifest);
    state = std::make_shared<SerializeState>();
}

void SerializationMapKeyPresenceMerge::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (!state)
        return;

    auto * write_state = checkAndGetState<SerializeState>(state);
    if (write_state->pending_rows > 0)
        flushPendingPresence(settings, *write_state);
}

void SerializationMapKeyPresenceMerge::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);

    settings.path.push_back(Substream::MapKeysInfo);
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        if (dynamic_cast<const DeserializeState *>(cached_state.get()))
        {
            state = cached_state;
            settings.path.pop_back();
            return;
        }
        if (const auto * cached_map = dynamic_cast<const SerializationMapWithKeyColumns::DeserializeBinaryBulkStateMapWithKeyColumns *>(cached_state.get()))
        {
            auto presence_state = std::make_shared<DeserializeState>();
            presence_state->manifest = cached_map->manifest;
            settings.path.pop_back();
            state = std::move(presence_state);
            return;
        }
    }

    auto * stream = settings.getter(settings.path);
    if (!stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Missing stream for Map keys info");

    auto presence_state = std::make_shared<DeserializeState>();
    presence_state->manifest = SerializationMapWithKeyColumns::readManifest(*stream, key_columns.getKeySerialization());
    addToSubstreamsDeserializeStatesCache(cache, settings.path, presence_state);
    settings.path.pop_back();
    state = std::move(presence_state);
}

void SerializationMapKeyPresenceMerge::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (write_manifest.keys.empty())
        return;

    auto * write_state = checkAndGetState<SerializeState>(state);
    const auto & array = assert_cast<const ColumnArray &>(column);
    const auto & nested = assert_cast<const ColumnUInt8 &>(array.getData());
    const auto & offsets = array.getOffsets();
    const size_t column_size = array.size();

    if (offset > column_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Presence merge offset {} is greater than column size {}", offset, column_size);

    const size_t rows = limit && offset + limit < column_size ? limit : column_size - offset;
    const size_t key_count = write_manifest.keys.size();

    if (write_state->pending_presence.empty())
        write_state->pending_presence.assign(key_count, {});
    else if (write_state->pending_presence.size() != key_count)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Map key presence key count changed from {} to {} inside a granule",
            write_state->pending_presence.size(),
            key_count);

    for (size_t row = 0; row < rows; ++row)
    {
        const size_t begin = offsets[static_cast<ssize_t>(offset + row) - 1];
        const size_t end = offsets[offset + row];
        if (end - begin != key_count)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Presence array row {} has {} values, expected {}",
                offset + row,
                end - begin,
                key_count);

        for (size_t key_i = 0; key_i < key_count; ++key_i)
            write_state->pending_presence[key_i].push_back(nested.getData()[begin + key_i]);
    }
    write_state->pending_rows += rows;

    if (settings.granule_is_complete)
        flushPendingPresence(settings, *write_state);
}

void SerializationMapKeyPresenceMerge::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache *) const
{
    auto * presence_state = checkAndGetState<DeserializeState>(state);
    auto & array = assert_cast<ColumnArray &>(column);
    auto & nested = assert_cast<ColumnUInt8 &>(array.getData());
    auto & offsets = array.getOffsets();

    const size_t key_count = presence_state->manifest.keys.size();
    if (key_count == 0)
    {
        for (size_t row = 0; row < limit; ++row)
            offsets.push_back(nested.size());
        return;
    }

    settings.path.push_back(Substream::MapKeyPresence);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Missing stream for Map key presence");

    std::vector<std::vector<UInt8>> presence;
    MapKeyPresenceBlock::deserializeAll(*stream, limit, key_count, presence);

    size_t rows = limit;
    if (!presence.empty())
        rows = presence.front().size();

    nested.getData().reserve(nested.size() + rows * key_count);
    offsets.reserve(offsets.size() + rows);

    for (size_t row = 0; row < rows; ++row)
    {
        for (size_t key_i = 0; key_i < key_count; ++key_i)
            nested.getData().push_back(presence[key_i][row]);
        offsets.push_back(nested.size());
    }
}

}
