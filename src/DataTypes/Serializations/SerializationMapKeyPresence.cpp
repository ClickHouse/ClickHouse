#include <DataTypes/Serializations/SerializationMapKeyPresence.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

struct DeserializeBinaryBulkStateMapKeyPresence : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr with_key_columns_state;
    ISerialization::DeserializeBinaryBulkStatePtr exists_state;
    ssize_t key_index = -1;
    MapKeyPresenceKind presence_kind = MapKeyPresenceKind::Tracked;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapKeyPresence>(*this);
        new_state->with_key_columns_state = with_key_columns_state ? with_key_columns_state->clone() : nullptr;
        new_state->exists_state = exists_state ? exists_state->clone() : nullptr;
        return new_state;
    }
};

void SerializationMapKeyPresence::throwNoSerialization()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for Map key presence subcolumn");
}

SerializationMapKeyPresence::SerializationMapKeyPresence(const SerializationPtr & map_with_key_columns_serialization_, Field key_, bool write_only_)
    : map_with_key_columns_serialization(map_with_key_columns_serialization_)
    , key(std::move(key_))
    , key_name(assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization).keyToStreamName(key))
    , write_only(write_only_)
{
}

SerializationPtr SerializationMapKeyPresence::create(const SerializationPtr & map_with_key_columns_serialization_, Field key_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapKeyPresence(map_with_key_columns_serialization_, std::move(key_), /*write_only_=*/ false));
}

SerializationPtr SerializationMapKeyPresence::createForWrite(const SerializationPtr & map_with_key_columns_serialization_, Field key_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapKeyPresence(map_with_key_columns_serialization_, std::move(key_), /*write_only_=*/ true));
}

void SerializationMapKeyPresence::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData &) const
{
    /// A key present in the manifest has its own `.exists_<name>` stream; a key absent from
    /// the manifest reads as all-0 and has no data files. Stream discovery runs before the
    /// manifest is known, so guard the stream with `check_stream_exists`: it is enumerated
    /// only when it exists on disk.
    const bool have_exists_check = settings.check_stream_exists_callback != nullptr;

    settings.path.push_back(Substream::MapKeyExists);
    settings.path.back().name_of_substream = key_name;
    const bool dedicated_exists = !have_exists_check || settings.check_stream_exists_callback(settings.path);
    if (dedicated_exists)
        callback(settings.path);
    settings.path.pop_back();
}

void SerializationMapKeyPresence::serializeBinaryBulkStatePrefix(
    const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    if (!write_only)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for read-only SerializationMapKeyPresence");

    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);
    settings.path.push_back(Substream::MapKeyExists);
    settings.path.back().name_of_substream = key_name;
    key_columns.getExistsSerialization()->serializeBinaryBulkStatePrefix(column, settings, state);
    settings.path.pop_back();
}

void SerializationMapKeyPresence::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    if (!write_only)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for read-only SerializationMapKeyPresence");

    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);
    settings.path.push_back(Substream::MapKeyExists);
    settings.path.back().name_of_substream = key_name;
    key_columns.getExistsSerialization()->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}

void SerializationMapKeyPresence::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    if (!write_only)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for read-only SerializationMapKeyPresence");

    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);
    settings.path.push_back(Substream::MapKeyExists);
    settings.path.back().name_of_substream = key_name;
    key_columns.getExistsSerialization()->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
    settings.path.pop_back();
}

void SerializationMapKeyPresence::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto presence_state = std::make_shared<DeserializeBinaryBulkStateMapKeyPresence>();
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);

    settings.path.push_back(ISerialization::Substream::MapKeysInfo);
    MapKeyManifest manifest;
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        if (const auto * cached = dynamic_cast<const SerializationMapWithKeyColumns::DeserializeBinaryBulkStateMapWithKeyColumns *>(cached_state.get()))
            manifest = cached->manifest;
        presence_state->with_key_columns_state = cached_state;
    }
    else
    {
        if (!settings.map_key_columns_manifest)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Missing key_columns manifest for with_key_columns Map");
        manifest = *settings.map_key_columns_manifest;
        auto stored = std::make_shared<SerializationMapWithKeyColumns::DeserializeBinaryBulkStateMapWithKeyColumns>();
        stored->manifest = manifest;
        addToSubstreamsDeserializeStatesCache(cache, settings.path, stored);
        presence_state->with_key_columns_state = stored;
    }
    settings.path.pop_back();

    for (size_t i = 0; i < manifest.keys.size(); ++i)
    {
        if (manifest.keys[i].key != key)
            continue;

        presence_state->key_index = static_cast<ssize_t>(i);
        presence_state->presence_kind = manifest.keys[i].presence_kind;

        if (presence_state->presence_kind == MapKeyPresenceKind::Tracked)
        {
            settings.path.push_back(ISerialization::Substream::MapKeyExists);
            settings.path.back().name_of_substream = key_columns.keyToStreamName(key);
            key_columns.getExistsSerialization()->deserializeBinaryBulkStatePrefix(settings, presence_state->exists_state, cache);
            settings.path.pop_back();
        }
        break;
    }

    state = std::move(presence_state);
}

void SerializationMapKeyPresence::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * presence_state = checkAndGetState<DeserializeBinaryBulkStateMapKeyPresence>(state);
    auto & result = assert_cast<ColumnUInt8 &>(column).getData();

    /// Key absent from this part: all rows absent.
    if (presence_state->key_index < 0)
    {
        result.resize_fill(result.size() + limit, 0);
        return;
    }

    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);

    /// `AlwaysPresent` keys have no `.exists_` stream: every row is present.
    if (presence_state->presence_kind == MapKeyPresenceKind::AlwaysPresent)
    {
        result.resize_fill(result.size() + limit, 1);
        return;
    }

    /// Read only this key's own presence stream, granule/mark aligned, shared with
    /// sibling reads (e.g. a full `m` read) through `cache`. Publish a fresh column
    /// (distinct from the result) so a later range never inserts it into itself.
    settings.path.push_back(Substream::MapKeyExists);
    settings.path.back().name_of_substream = key_columns.keyToStreamName(key);
    if (!insertDataFromSubstreamsCacheIfAny(cache, settings, column))
    {
        auto exists_column = ColumnUInt8::create();
        key_columns.getExistsSerialization()->deserializeBinaryBulkWithMultipleStreams(
            *exists_column, limit, settings, presence_state->exists_state, cache);
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, exists_column->getPtr(), exists_column->size());
        column.insertRangeFrom(*exists_column, 0, exists_column->size());
    }
    settings.path.pop_back();
}

}
