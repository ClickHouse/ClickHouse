#include <DataTypes/Serializations/SerializationMapWithKeyColumnsValue.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>

#include <Columns/ColumnArray.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

struct DeserializeBinaryBulkStateMapWithKeyColumnsValue : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr with_key_columns_state;
    ISerialization::DeserializeBinaryBulkStatePtr value_state;
    bool key_in_manifest = false;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumnsValue>(*this);
        new_state->with_key_columns_state = with_key_columns_state ? with_key_columns_state->clone() : nullptr;
        new_state->value_state = value_state ? value_state->clone() : nullptr;
        return new_state;
    }
};

struct SerializeBinaryBulkStateMapWithKeyColumnsValue : public ISerialization::SerializeBinaryBulkState
{
    ISerialization::SerializeBinaryBulkStatePtr value_state;
};

SerializationMapWithKeyColumnsValue::SerializationMapWithKeyColumnsValue(
    const SerializationPtr & value_serialization_,
    const DataTypePtr & value_type_,
    const SerializationPtr & map_with_key_columns_serialization_,
    Field key_,
    bool write_value_only_)
    : SerializationWrapper(value_serialization_)
    , value_type(value_type_)
    , map_with_key_columns_serialization(map_with_key_columns_serialization_)
    , key(std::move(key_))
    , key_name(assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization).keyToStreamName(key))
    , write_value_only(write_value_only_)
{
}

SerializationPtr SerializationMapWithKeyColumnsValue::create(
    const SerializationPtr & value_serialization_,
    const DataTypePtr & value_type_,
    const SerializationPtr & map_with_key_columns_serialization_,
    Field key_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapWithKeyColumnsValue(
        value_serialization_, value_type_, map_with_key_columns_serialization_, std::move(key_), /*write_value_only_=*/ false));
}

SerializationPtr SerializationMapWithKeyColumnsValue::createForWrite(
    const SerializationPtr & value_serialization_,
    const DataTypePtr & value_type_,
    const SerializationPtr & map_with_key_columns_serialization_,
    Field key_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapWithKeyColumnsValue(
        value_serialization_, value_type_, map_with_key_columns_serialization_, std::move(key_), /*write_value_only_=*/ true));
}

void SerializationMapWithKeyColumnsValue::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    if (!write_value_only)
    {
        settings.path.push_back(Substream::MapKeysInfo);
        callback(settings.path);
        settings.path.pop_back();
    }

    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_name;
    if (!settings.check_stream_exists_callback || settings.check_stream_exists_callback(settings.path))
    {
        auto value_data = SubstreamData(nested_serialization).withType(value_type).withColumn(data.column);
        nested_serialization->enumerateStreams(settings, callback, value_data);
    }
    settings.path.pop_back();
}

void SerializationMapWithKeyColumnsValue::serializeBinaryBulkStatePrefix(
    const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    if (!write_value_only)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationMapWithKeyColumnsValue");

    auto value_state = std::make_shared<SerializeBinaryBulkStateMapWithKeyColumnsValue>();
    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_name;
    nested_serialization->serializeBinaryBulkStatePrefix(column, settings, value_state->value_state);
    settings.path.pop_back();
    state = std::move(value_state);
}

void SerializationMapWithKeyColumnsValue::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    if (!write_value_only)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationMapWithKeyColumnsValue");

    if (!state)
        return;

    auto * value_state = checkAndGetState<SerializeBinaryBulkStateMapWithKeyColumnsValue>(state);
    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_name;
    nested_serialization->serializeBinaryBulkStateSuffix(settings, value_state->value_state);
    settings.path.pop_back();
}

void SerializationMapWithKeyColumnsValue::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    if (!write_value_only)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationMapWithKeyColumnsValue");

    auto * value_state = checkAndGetState<SerializeBinaryBulkStateMapWithKeyColumnsValue>(state);
    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_name;
    if (const auto * nested_array = typeid_cast<const ColumnArray *>(&column))
    {
        if (typeid_cast<const ColumnArray *>(&nested_array->getData()))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Per-key Map value write for key {} expected {}, got {}",
                key_name,
                value_type->getName(),
                column.dumpStructure());
    }
    nested_serialization->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, value_state->value_state);
    settings.path.pop_back();
}

void SerializationMapWithKeyColumnsValue::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto value_state = std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumnsValue>();
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);

    settings.path.push_back(ISerialization::Substream::MapKeysInfo);

    MapKeyManifest manifest;
    const SerializationMapWithKeyColumns::DeserializeBinaryBulkStateMapWithKeyColumns * cached_map = nullptr;
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        cached_map = dynamic_cast<const SerializationMapWithKeyColumns::DeserializeBinaryBulkStateMapWithKeyColumns *>(cached_state.get());
        if (cached_map)
            manifest = cached_map->manifest;
        value_state->with_key_columns_state = cached_state;
    }
    else
    {
        auto * stream = settings.getter(settings.path);
        if (!stream)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Missing stream for Map keys info");

        manifest = SerializationMapWithKeyColumns::readManifest(*stream, key_columns.getKeySerialization());
        auto stored = std::make_shared<SerializationMapWithKeyColumns::DeserializeBinaryBulkStateMapWithKeyColumns>();
        stored->manifest = manifest;
        addToSubstreamsDeserializeStatesCache(cache, settings.path, stored);
        value_state->with_key_columns_state = stored;
    }
    settings.path.pop_back();

    for (size_t i = 0; i < manifest.keys.size(); ++i)
    {
        if (manifest.keys[i].key != key)
            continue;

        value_state->key_in_manifest = true;
        if (cached_map && i < cached_map->value_states.size() && cached_map->value_states[i])
        {
            value_state->value_state = cached_map->value_states[i];
            break;
        }

        settings.path.push_back(ISerialization::Substream::MapKey);
        settings.path.back().name_of_substream = key_name;
        nested_serialization->deserializeBinaryBulkStatePrefix(settings, value_state->value_state, cache);
        settings.path.pop_back();
        break;
    }

    state = std::move(value_state);
}

void SerializationMapWithKeyColumnsValue::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * value_state = checkAndGetState<DeserializeBinaryBulkStateMapWithKeyColumnsValue>(state);
    if (!value_state->key_in_manifest)
    {
        column.insertManyDefaults(limit);
        return;
    }

    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_name;
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(column, limit, settings, value_state->value_state, cache);
    settings.path.pop_back();
}

}
