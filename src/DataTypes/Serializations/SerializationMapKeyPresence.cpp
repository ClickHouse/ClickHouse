#include <DataTypes/Serializations/SerializationMapKeyPresence.h>
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

struct DeserializeBinaryBulkStateMapKeyPresence : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr with_key_columns_state;
    ssize_t key_index = -1;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapKeyPresence>(*this);
        new_state->with_key_columns_state = with_key_columns_state ? with_key_columns_state->clone() : nullptr;
        return new_state;
    }
};

void SerializationMapKeyPresence::throwNoSerialization()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for Map key presence subcolumn");
}

SerializationMapKeyPresence::SerializationMapKeyPresence(const SerializationPtr & map_with_key_columns_serialization_, Field key_)
    : map_with_key_columns_serialization(map_with_key_columns_serialization_)
    , key(std::move(key_))
{
}

SerializationPtr SerializationMapKeyPresence::create(const SerializationPtr & map_with_key_columns_serialization_, Field key_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapKeyPresence(map_with_key_columns_serialization_, std::move(key_)));
}

void SerializationMapKeyPresence::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData &) const
{
    settings.path.push_back(Substream::MapKeysInfo);
    callback(settings.path);
    settings.path.pop_back();

    settings.path.push_back(Substream::MapKeyPresence);
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);
    settings.path.back().name_of_substream = key_columns.keyToStreamName(key);
    if (!settings.check_stream_exists_callback || settings.check_stream_exists_callback(settings.path))
        callback(settings.path);
    settings.path.pop_back();
}

void SerializationMapKeyPresence::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationMapKeyPresence");
}

void SerializationMapKeyPresence::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationMapKeyPresence");
}

void SerializationMapKeyPresence::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationMapKeyPresence");
}

void SerializationMapKeyPresence::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto presence_state = std::make_shared<DeserializeBinaryBulkStateMapKeyPresence>();
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);
    key_columns.deserializeBinaryBulkStatePrefix(settings, presence_state->with_key_columns_state, cache);

    const auto & manifest = key_columns.getManifestFromState(presence_state->with_key_columns_state);
    for (size_t i = 0; i < manifest.keys.size(); ++i)
    {
        if (manifest.keys[i].key == key)
        {
            presence_state->key_index = static_cast<ssize_t>(i);
            break;
        }
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
    (void)cache;
    auto * presence_state = checkAndGetState<DeserializeBinaryBulkStateMapKeyPresence>(state);
    auto & result = assert_cast<ColumnUInt8 &>(column).getData();

    if (presence_state->key_index < 0)
    {
        result.resize_fill(result.size() + limit, 0);
        return;
    }

    settings.path.push_back(Substream::MapKeyPresence);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Missing stream for Map key presence");

    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);
    const auto & manifest = key_columns.getManifestFromState(presence_state->with_key_columns_state);
    std::vector<UInt8> bits;
    MapKeyPresenceBlock::deserializeKey(
        *stream, limit, manifest.keys.size(), static_cast<size_t>(presence_state->key_index), bits);
    result.insert(result.end(), bits.begin(), bits.end());
}

}
