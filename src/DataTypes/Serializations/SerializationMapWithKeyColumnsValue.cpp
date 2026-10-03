#include <DataTypes/Serializations/SerializationMapWithKeyColumnsValue.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>
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
    /// A key present in the manifest has its own `.key_<name>` value stream. A key absent
    /// from the manifest has no data files and reads as all-default, so its streams are not
    /// enumerated. Stream discovery runs before the manifest is known, so guard the value
    /// stream with `check_stream_exists`: it is enumerated only when it exists on disk.
    const bool have_exists_check = settings.check_stream_exists_callback != nullptr;

    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_name;
    const bool dedicated_exists = !have_exists_check || settings.check_stream_exists_callback(settings.path);
    if (dedicated_exists)
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
        if (!settings.map_key_columns_manifest)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Missing key_columns manifest for with_key_columns Map");

        manifest = *settings.map_key_columns_manifest;
        auto stored = std::make_shared<SerializationMapWithKeyColumns::DeserializeBinaryBulkStateMapWithKeyColumns>();
        stored->manifest = manifest;
        addToSubstreamsDeserializeStatesCache(cache, settings.path, stored);
        value_state->with_key_columns_state = stored;
    }
    settings.path.pop_back();

    for (const auto & entry : manifest.keys)
    {
        if (entry.key != key)
            continue;

        value_state->key_in_manifest = true;

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
    if (!insertDataFromSubstreamsCacheIfAny(cache, settings, column))
    {
        /// Read into a fresh column and publish that, then append to the result.
        /// The cached column must be distinct from any reader's result column,
        /// otherwise the next range would insert the column into itself.
        auto value_column = value_type->createColumn();
        nested_serialization->deserializeBinaryBulkWithMultipleStreams(*value_column, limit, settings, value_state->value_state, cache);
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, value_column->getPtr(), value_column->size());
        column.insertRangeFrom(*value_column, 0, value_column->size());
    }
    settings.path.pop_back();
}

}
