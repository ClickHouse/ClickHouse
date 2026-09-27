#include <DataTypes/Serializations/SerializationMapKeyValue.h>
#include <DataTypes/Serializations/SerializationMap.h>

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
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


SerializationMapKeyExists::SerializationMapKeyExists(
    const SerializationPtr & uint8_serialization_,
    const SerializationPtr & map_nested_serialization_,
    MergeTreeMapSerializationVersion serialization_version_,
    ColumnPtr key_,
    const DataTypePtr & nested_type_)
    : SerializationWrapper(uint8_serialization_)
    , map_nested_serialization(map_nested_serialization_)
    , serialization_version(serialization_version_)
    , key(std::move(key_))
    , nested_type(nested_type_)
{
}

SerializationPtr SerializationMapKeyExists::create(
    const SerializationPtr & map_nested_serialization_,
    MergeTreeMapSerializationVersion serialization_version_,
    ColumnPtr key_,
    const DataTypePtr & nested_type_)
{
    auto uint8_serialization = std::make_shared<DataTypeUInt8>()->getDefaultSerialization();
    return std::shared_ptr<ISerialization>(new SerializationMapKeyExists(
        uint8_serialization, map_nested_serialization_, serialization_version_, std::move(key_), nested_type_));
}

struct DeserializeBinaryBulkStateMapKeyExists : public ISerialization::DeserializeBinaryBulkState
{
    size_t bucket = 0;
    ISerialization::DeserializeBinaryBulkStatePtr nested_state;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapKeyExists>(*this);
        new_state->nested_state = nested_state ? nested_state->clone() : nullptr;
        return new_state;
    }
};

void SerializationMapKeyExists::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    const auto * state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateMapKeyExists>(data.deserialize_state) : nullptr;

    auto next_data = SubstreamData(map_nested_serialization)
        .withType(data.type ? nested_type : nullptr)
        .withColumn(data.column ? nested_type->createColumn() : nullptr)
        .withSerializationInfo(data.serialization_info)
        .withDeserializeState(state ? state->nested_state : nullptr);

    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        map_nested_serialization->enumerateStreams(settings, callback, next_data);
        return;
    }

    settings.path.push_back(Substream::MapBucketsInfo);
    callback(settings.path);
    settings.path.pop_back();

    if (!state)
        return;

    settings.path.push_back(SubstreamType::Bucket);
    settings.path.back().bucket = state->bucket;
    map_nested_serialization->enumerateStreams(settings, callback, next_data);
    settings.path.pop_back();
}

void SerializationMapKeyExists::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationMapKeyExists");
}

void SerializationMapKeyExists::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationMapKeyExists");
}

void SerializationMapKeyExists::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationMapKeyExists");
}

void SerializationMapKeyExists::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    auto exists_state = std::make_shared<DeserializeBinaryBulkStateMapKeyExists>();

    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        map_nested_serialization->deserializeBinaryBulkStatePrefix(settings, exists_state->nested_state, cache);
        state = std::move(exists_state);
        return;
    }

    auto buckets_info_state = SerializationMap::deserializeBucketsInfoStatePrefix(settings, cache);
    const auto * buckets_info_state_concrete = checkAndGetState<SerializationMap::DeserializeBinaryBulkStateBucketsInfo>(buckets_info_state);
    exists_state->bucket = SerializationMap::getBucketForKey(key, 0, buckets_info_state_concrete->buckets);

    settings.path.push_back(SubstreamType::Bucket);
    settings.path.back().bucket = exists_state->bucket;
    map_nested_serialization->deserializeBinaryBulkStatePrefix(settings, exists_state->nested_state, cache);
    settings.path.pop_back();

    state = std::move(exists_state);
}

void SerializationMapKeyExists::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    ColumnPtr nested_column;
    size_t num_read_rows = 0;
    auto * exists_state = checkAndGetState<DeserializeBinaryBulkStateMapKeyExists>(state);

    if (serialization_version == MergeTreeMapSerializationVersion::WITH_BUCKETS)
    {
        settings.path.push_back(SubstreamType::Bucket);
        settings.path.back().bucket = exists_state->bucket;
    }

    if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
    {
        std::tie(nested_column, num_read_rows) = *cached_column_with_num_read_rows;
    }
    else
    {
        auto mutable_nested_column = nested_type->createColumn();
        map_nested_serialization->deserializeBinaryBulkWithMultipleStreams(*mutable_nested_column, limit, settings, exists_state->nested_state, cache);
        num_read_rows = mutable_nested_column->size();
        nested_column = std::move(mutable_nested_column);
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, nested_column, num_read_rows);
    }

    extractKeyPresenceFromMap(*nested_column, *key, column, nested_column->size() - num_read_rows, nested_column->size());

    if (serialization_version == MergeTreeMapSerializationVersion::WITH_BUCKETS)
        settings.path.pop_back();
}


}
