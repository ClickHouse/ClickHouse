#include <DataTypes/Serializations/SerializationMapKeyValue.h>
#include <DataTypes/Serializations/SerializationMap.h>

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <Common/logger_useful.h>

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
    /// Shared reading info state, cached at the substream path.
    /// Contains `reading_full_map` flag set by `SerializationMap`.
    ISerialization::DeserializeBinaryBulkStatePtr reading_info_state;
    /// Total number of buckets in this Map column.
    size_t buckets = 1;
    /// The specific bucket that contains the requested key (determined by hashing the key).
    size_t bucket = 0;
    /// Nested deserialization state for the selected bucket's sub-stream.
    ISerialization::DeserializeBinaryBulkStatePtr nested_state;
    /// Cached deserialized nested column. Used to accumulate data across granules
    /// when the full Map is also being read (for cache sharing with `SerializationMap`).
    ColumnPtr nested_column;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapKeyValue>(*this);
        new_state->reading_info_state = reading_info_state ? reading_info_state->clone() : nullptr;
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

    /// Get or create the shared reading info state at the current substream path.
    /// If `SerializationMap` has already initialized it, `reading_full_map` will be true.
    map_key_value_state->reading_info_state = SerializationMap::deserializeMapReadingInfoStatePrefix(cache, settings.path);

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
    map_key_value_state->buckets = buckets_info_state_concrete->buckets;
    map_key_value_state->bucket = SerializationMap::getBucketForKey(key, 0, buckets_info_state_concrete->buckets);

    /// Only initialize the nested state for the single bucket containing our key.
    settings.path.push_back(SubstreamType::Bucket);
    settings.path.back().bucket = map_key_value_state->bucket;
    map_nested_serialization->deserializeBinaryBulkStatePrefix(settings, map_key_value_state->nested_state, cache);
    settings.path.pop_back();

    state = std::move(map_key_value_state);
}

void SerializationMapKeyValue::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    ColumnPtr nested_column;
    size_t num_read_rows = 0;
    auto * map_key_value_state = checkAndGetState<DeserializeBinaryBulkStateMapKeyValue>(state);
    const auto * reading_info = checkAndGetState<SerializationMap::DeserializeBinaryBulkStateMapReadingInfo>(map_key_value_state->reading_info_state);

    /// For the bucketed format, push the target bucket onto the path so that
    /// the nested serialization and substreams cache use the correct sub-stream.
    if (serialization_version == MergeTreeMapSerializationVersion::WITH_BUCKETS)
    {
        settings.path.push_back(SubstreamType::Bucket);
        settings.path.back().bucket = map_key_value_state->bucket;
    }

    /// For BASIC format or single-bucket case, the whole Map is stored in a single stream.
    /// Use the substreams cache to share the deserialized Map column with `SerializationMap`
    /// (when the query reads both the full Map and a key subcolumn at the same time).
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC || map_key_value_state->buckets == 1)
    {
        /// Try to get the already-deserialized Map column from the substreams cache.
        if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
        {
            std::tie(nested_column, num_read_rows) = *cached_column_with_num_read_rows;
        }
        /// The full Map is also being read — accumulate data in the state so the
        /// substreams cache can share the column with `SerializationMap`.
        else if (reading_info->reading_full_map)
        {
            if (column->empty() || !map_key_value_state->nested_column)
                map_key_value_state->nested_column = nested_type->createColumn();

            size_t prev_size = map_key_value_state->nested_column->size();
            auto settings_copy = settings;
            /// In Compact part each granule is deserialized with new deserialize state,
            /// so we always have empty nested_column. If we also read another subcolumn from the same Map
            /// (like keys/values), we cannot use whole columns from substreams cache here, because we will
            /// get wrong size for nested_column (substreams cache works per block, not per granule).
            if (settings.data_part_type == MergeTreeDataPartType::Compact)
                settings_copy.insert_only_rows_in_current_range_from_substreams_cache = true;
            map_nested_serialization->deserializeBinaryBulkWithMultipleStreams(map_key_value_state->nested_column, rows_offset, limit, settings_copy, map_key_value_state->nested_state, cache);
            num_read_rows = map_key_value_state->nested_column->size() - prev_size;
            addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, map_key_value_state->nested_column, num_read_rows);
            nested_column = map_key_value_state->nested_column;
        }
        /// Only this subcolumn is being read. Deserialize into a fresh column
        /// each time to avoid accumulating the full Map data in memory across granules.
        else
        {
            nested_column = nested_type->createColumn();
            auto settings_copy = settings;
            /// We need only rows from current range to be inserted into temporary nested column.
            settings_copy.insert_only_rows_in_current_range_from_substreams_cache = true;
            map_nested_serialization->deserializeBinaryBulkWithMultipleStreams(nested_column, rows_offset, limit, settings_copy, map_key_value_state->nested_state, cache);
            num_read_rows = nested_column->size();
        }
    }
    /// Multiple buckets. Only the bucket containing our key was opened, so read it directly.
    /// No caching is needed here because the bucket sub-stream is not shared with `SerializationMap`
    /// (which reads all buckets).
    else
    {
        nested_column = nested_type->createColumn();
        map_nested_serialization->deserializeBinaryBulkWithMultipleStreams(nested_column, rows_offset, limit, settings, map_key_value_state->nested_state, cache);
        num_read_rows = nested_column->size();
    }

    /// Extract the value for the requested key from the deserialized Map data.
    extractKeyValueFromMap(*nested_column, *key, *column->assumeMutable(), nested_column->size() - num_read_rows, nested_column->size());

    if (serialization_version == MergeTreeMapSerializationVersion::WITH_BUCKETS)
        settings.path.pop_back();
}


}
