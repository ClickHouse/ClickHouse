#include <DataTypes/Serializations/SerializationMapWithKeyColumnsValue.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>

#include <Common/Exception.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
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

SerializationMapWithKeyColumnsValue::SerializationMapWithKeyColumnsValue(
    const SerializationPtr & value_serialization_,
    const DataTypePtr & value_type_,
    const SerializationPtr & map_with_key_columns_serialization_,
    Field key_)
    : SerializationWrapper(value_serialization_)
    , value_type(value_type_)
    , map_with_key_columns_serialization(map_with_key_columns_serialization_)
    , key(std::move(key_))
    , key_name(assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization).keyToStreamName(key))
{
}

SerializationPtr SerializationMapWithKeyColumnsValue::create(
    const SerializationPtr & value_serialization_,
    const DataTypePtr & value_type_,
    const SerializationPtr & map_with_key_columns_serialization_,
    Field key_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapWithKeyColumnsValue(
        value_serialization_, value_type_, map_with_key_columns_serialization_, std::move(key_)));
}

void SerializationMapWithKeyColumnsValue::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    settings.path.push_back(Substream::MapKeysInfo);
    callback(settings.path);
    settings.path.pop_back();

    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_name;
    auto value_data = SubstreamData(nested_serialization).withType(value_type).withColumn(data.column);
    nested_serialization->enumerateStreams(settings, callback, value_data);
    settings.path.pop_back();
}

void SerializationMapWithKeyColumnsValue::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationMapWithKeyColumnsValue");
}

void SerializationMapWithKeyColumnsValue::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationMapWithKeyColumnsValue");
}

void SerializationMapWithKeyColumnsValue::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationMapWithKeyColumnsValue");
}

void SerializationMapWithKeyColumnsValue::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto value_state = std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumnsValue>();
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);
    key_columns.deserializeBinaryBulkStatePrefix(settings, value_state->with_key_columns_state, cache);

    const auto & manifest = key_columns.getManifestFromState(value_state->with_key_columns_state);
    for (size_t i = 0; i < manifest.keys.size(); ++i)
    {
        if (manifest.keys[i].key == key)
        {
            value_state->key_in_manifest = true;
            value_state->value_state = assert_cast<const SerializationMapWithKeyColumns::DeserializeBinaryBulkStateMapWithKeyColumns &>(*value_state->with_key_columns_state).value_states[i];
            break;
        }
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
