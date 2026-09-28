#include <DataTypes/Serializations/SerializationMapKeyValue.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <DataTypes/Serializations/SerializationMapKeyColumns.h>

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

SerializationMapKeyValue::SerializationMapKeyValue(
    const SerializationPtr & value_serialization_,
    const SerializationPtr & map_nested_serialization_,
    MergeTreeMapSerializationVersion serialization_version_,
    ColumnPtr key_,
    const DataTypePtr & nested_type_)
    : SerializationWrapper(value_serialization_)
    , map_nested_serialization(map_nested_serialization_)
    , serialization_version(serialization_version_)
    , key(std::move(key_))
    , nested_type(nested_type_)
{
}

SerializationPtr SerializationMapKeyValue::create(
    const SerializationPtr & value_serialization_,
    const SerializationPtr & map_nested_serialization_,
    MergeTreeMapSerializationVersion serialization_version_,
    ColumnPtr key_,
    const DataTypePtr & nested_type_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapKeyValue(value_serialization_, map_nested_serialization_, serialization_version_, std::move(key_), nested_type_));
}

SerializationPtr SerializationMapKeyValue::createKeyColumns(
    const SerializationPtr & value_serialization_,
    const SerializationPtr & map_nested_serialization_,
    String key_,
    String subcolumn_name_)
{
    /// `map_nested_serialization_` of a with_key_columns Map is the
    /// `SerializationMapKeyColumns` itself.
    return SerializationMapKeyColumn::create(
        value_serialization_,
        map_nested_serialization_,
        std::move(key_),
        std::move(subcolumn_name_));
}

SerializationPtr SerializationMapKeyValue::createKeyColumnsSerialization() const
{
    const auto * per_key = typeid_cast<const SerializationMapKeyColumns *>(map_nested_serialization.get());
    if (!per_key)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "with_key_columns Map subcolumns require the per-key Map serialization");

    if (key->size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a single key value for a Map key subcolumn");

    String key_string{key->getDataAt(0)};
    return SerializationMapKeyColumn::create(
        nested_serialization, per_key->getPtr(), key_string, String(DataTypeMap::KEY_SUBCOLUMN_PREFIX) + key_string);
}

/// Deserialization state for reading a single key's value from a Map.
/// For WITH_BUCKETS format, reads only one bucket (the one containing the requested key).
struct DeserializeBinaryBulkStateMapKeyValue : public ISerialization::DeserializeBinaryBulkState
{
    /// The specific bucket that contains the requested key (determined by hashing the key).
    size_t bucket = 0;
    /// Nested deserialization state for the selected bucket's sub-stream.
    ISerialization::DeserializeBinaryBulkStatePtr nested_state;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapKeyValue>(*this);
        new_state->nested_state = nested_state ? nested_state->clone() : nullptr;
        return new_state;
    }
};

void SerializationMapKeyValue::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    if (serialization_version == MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS)
    {
        createKeyColumnsSerialization()->enumerateStreams(settings, callback, data);
        return;
    }
    const auto * map_key_value_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateMapKeyValue>(data.deserialize_state) : nullptr;

    auto next_data = SubstreamData(map_nested_serialization)
        .withType(data.type ? nested_type : nullptr)
        .withColumn(data.column ? nested_type->createColumn() : nullptr)
        .withSerializationInfo(data.serialization_info)
        .withDeserializeState(map_key_value_state ? map_key_value_state->nested_state : nullptr);

    /// BASIC format has no bucketing, delegate directly.
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        map_nested_serialization->enumerateStreams(settings, callback, next_data);
        return;
    }

    /// The shared buckets info stream.
    settings.path.push_back(Substream::MapBucketsInfo);
    callback(settings.path);
    settings.path.pop_back();

    /// Need deserialization state to know which bucket the key belongs to.
    if (!map_key_value_state)
        return;

    /// Only enumerate the single bucket that contains the requested key.
    settings.path.push_back(SubstreamType::Bucket);
    settings.path.back().bucket = map_key_value_state->bucket;
    map_nested_serialization->enumerateStreams(settings, callback, next_data);
    settings.path.pop_back();
}

/// Serialization methods are not implemented because key-value subcolumns are read-only.
/// Writing always goes through `SerializationMap` which handles the full Map column.

void SerializationMapKeyValue::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationMapKeyValue");
}

void SerializationMapKeyValue::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationMapKeyValue");
}

void SerializationMapKeyValue::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationMapKeyValue");
}

void SerializationMapKeyValue::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    if (serialization_version == MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS)
    {
        createKeyColumnsSerialization()->deserializeBinaryBulkStatePrefix(settings, state, cache);
        return;
    }
    auto map_key_value_state = std::make_shared<DeserializeBinaryBulkStateMapKeyValue>();

    /// BASIC format has no bucketing, delegate directly.
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        map_nested_serialization->deserializeBinaryBulkStatePrefix(settings, map_key_value_state->nested_state, cache);
        state = std::move(map_key_value_state);
        return;
    }

    /// Read the bucket count and determine which bucket the requested key belongs to.
    auto buckets_info_state = SerializationMap::deserializeBucketsInfoStatePrefix(settings, cache);
    const auto * buckets_info_state_concrete = checkAndGetState<SerializationMap::DeserializeBinaryBulkStateBucketsInfo>(buckets_info_state);
    map_key_value_state->bucket = SerializationMap::getBucketForKey(key, 0, buckets_info_state_concrete->buckets);

    /// Only initialize the nested state for the single bucket containing our key.
    settings.path.push_back(SubstreamType::Bucket);
    settings.path.back().bucket = map_key_value_state->bucket;
    map_nested_serialization->deserializeBinaryBulkStatePrefix(settings, map_key_value_state->nested_state, cache);
    settings.path.pop_back();

    state = std::move(map_key_value_state);
}

void SerializationMapKeyValue::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (serialization_version == MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS)
    {
        createKeyColumnsSerialization()->deserializeBinaryBulkWithMultipleStreams(column, limit, settings, state, cache);
        return;
    }

    ColumnPtr nested_column;
    size_t num_read_rows = 0;
    auto * map_key_value_state = checkAndGetState<DeserializeBinaryBulkStateMapKeyValue>(state);

    /// For the bucketed format, read only the bucket that contains our key.
    if (serialization_version == MergeTreeMapSerializationVersion::WITH_BUCKETS)
    {
        settings.path.push_back(SubstreamType::Bucket);
        settings.path.back().bucket = map_key_value_state->bucket;
    }

    /// Reuse the nested Map column from the cache if SerializationMap or another key-value subcolumn already read this range.
    if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
    {
        std::tie(nested_column, num_read_rows) = *cached_column_with_num_read_rows;
    }
    /// Otherwise deserialize the whole nested Map and cache it for other key-value subcolumns.
    else
    {
        auto mutable_nested_column = nested_type->createColumn();
        map_nested_serialization->deserializeBinaryBulkWithMultipleStreams(*mutable_nested_column, limit, settings, map_key_value_state->nested_state, cache);
        num_read_rows = mutable_nested_column->size();
        nested_column = std::move(mutable_nested_column);
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, nested_column, num_read_rows);
    }

    /// Extract the value for the requested key from the deserialized Map data.
    extractKeyValueFromMap(*nested_column, *key, column, nested_column->size() - num_read_rows, nested_column->size());

    if (serialization_version == MergeTreeMapSerializationVersion::WITH_BUCKETS)
        settings.path.pop_back();
}


}
