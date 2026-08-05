#include <DataTypes/Serializations/SerializationMapKeysOrValues.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Common/SipHash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

SerializationMapKeysOrValues::SerializationMapKeysOrValues(
    const SerializationPtr & keys_or_values_serialization_, MergeTreeMapSerializationVersion serialization_version_)
    : keys_or_values_serialization(keys_or_values_serialization_)
    , serialization_version(serialization_version_)
{
}

UInt128 SerializationMapKeysOrValues::getHash(const SerializationPtr & keys_or_values_serialization_, MergeTreeMapSerializationVersion serialization_version_)
{
    SipHash hash;
    hash.update("MapKeysOrValues");
    hash.update(keys_or_values_serialization_->getHash());
    hash.update(static_cast<UInt8>(serialization_version_));
    return hash.get128();
}

SerializationPtr SerializationMapKeysOrValues::create(const SerializationPtr & keys_or_values_serialization_, MergeTreeMapSerializationVersion serialization_version_)
{
    if (!keys_or_values_serialization_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationMapKeysOrValues(keys_or_values_serialization_, serialization_version_));
    return ISerialization::pooled(getHash(keys_or_values_serialization_, serialization_version_), [&] { return new SerializationMapKeysOrValues(keys_or_values_serialization_, serialization_version_); });
}

/// Deserialization state for the bucketed Map keys/values subcolumn.
/// Mirrors the structure of `DeserializeBinaryBulkStateMap` but
/// holds nested states for keys or values only (not the full Map).
struct DeserializeBinaryBulkStateMapKeysOrValuesWithBuckets : public ISerialization::DeserializeBinaryBulkState
{
    /// State from the shared buckets info stream (bucket count and optional statistics).
    ISerialization::DeserializeBinaryBulkStatePtr buckets_info_state;
    /// Per-bucket deserialization state for the keys or values sub-stream.
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> bucket_keys_or_values_states;

    /// Bucket index stream state (used to preserve original key order).
    DataTypePtr bucket_index_type;
    SerializationPtr bucket_index_serialization;
    ISerialization::DeserializeBinaryBulkStatePtr bucket_index_state;
    bool has_bucket_index = false;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapKeysOrValuesWithBuckets>(*this);
        new_state->buckets_info_state = buckets_info_state ? buckets_info_state->clone() : nullptr;
        for (size_t bucket = 0; bucket != bucket_keys_or_values_states.size(); ++bucket)
            new_state->bucket_keys_or_values_states[bucket] = bucket_keys_or_values_states[bucket] ? bucket_keys_or_values_states[bucket]->clone() : nullptr;
        new_state->bucket_index_state = bucket_index_state ? bucket_index_state->clone() : nullptr;
        return new_state;
    }

    void forEachNestedState(const std::function<void(const ISerialization::DeserializeBinaryBulkStatePtr &)> & callback) const override
    {
        if (buckets_info_state)
            callback(buckets_info_state);
        for (const auto & bucket_state : bucket_keys_or_values_states)
        {
            if (bucket_state)
                callback(bucket_state);
        }
    }
};


void SerializationMapKeysOrValues::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    auto next_data = SubstreamData(keys_or_values_serialization)
        .withType(data.type)
        .withColumn(data.column)
        .withSerializationInfo(data.serialization_info)
        .withDeserializeState(data.deserialize_state);

    /// BASIC format has no bucketing, delegate directly.
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        keys_or_values_serialization->enumerateStreams(settings, callback, next_data);
        return;
    }

    /// The buckets info stream.
    settings.path.push_back(Substream::MapBucketsInfo);
    callback(settings.path);
    settings.path.pop_back();

    /// Need deserialization state to know the bucket count.
    if (!data.deserialize_state)
        return;

    const auto * map_keys_or_values_with_buckets_deserialize_state = checkAndGetState<DeserializeBinaryBulkStateMapKeysOrValuesWithBuckets>(data.deserialize_state) ;
    const auto * buckets_info_state = checkAndGetState<SerializationMap::DeserializeBinaryBulkStateBucketsInfo>(map_keys_or_values_with_buckets_deserialize_state->buckets_info_state);

    /// Enumerate the bucket index stream (used to preserve original key order).
    /// Only needed when there are multiple buckets. When a check_stream_exists_callback
    /// is set, skip the stream if it does not exist in the part — old parts written
    /// before the bucket index fix lack this stream.
    if (buckets_info_state->buckets > 1)
    {
        settings.path.push_back(Substream::MapBucketIndexes);
        bool enumerate_bucket_index = !settings.check_stream_exists_callback || settings.check_stream_exists_callback(settings.path);
        if (enumerate_bucket_index)
        {
            auto bucket_index_serialization = getSmallestIndexesType(buckets_info_state->buckets)->getDefaultSerialization();
            auto bucket_index_data = SubstreamData(bucket_index_serialization)
                .withDeserializeState(map_keys_or_values_with_buckets_deserialize_state->bucket_index_state);
            bucket_index_serialization->enumerateStreams(settings, callback, bucket_index_data);
        }
        settings.path.pop_back();
    }

    /// Enumerate a keys/values sub-stream for each bucket.
    for (size_t bucket = 0; bucket < buckets_info_state->buckets; ++bucket)
    {
        settings.path.push_back(SubstreamType::Bucket);
        settings.path.back().bucket = bucket;
        next_data.deserialize_state =  map_keys_or_values_with_buckets_deserialize_state->bucket_keys_or_values_states[bucket];
        keys_or_values_serialization->enumerateStreams(settings, callback, next_data);
        settings.path.pop_back();
    }
}

/// Serialization methods are not implemented because keys/values subcolumns are read-only.
/// Writing always goes through `SerializationMap` which handles the full Map column.

void SerializationMapKeysOrValues::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationMapKeysOrValues");
}

void SerializationMapKeysOrValues::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationMapKeysOrValues");
}

void SerializationMapKeysOrValues::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationMapKeysOrValues");
}

void SerializationMapKeysOrValues::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    /// BASIC format has no bucketing, delegate directly.
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        keys_or_values_serialization->deserializeBinaryBulkStatePrefix(settings, state, cache);
        return;
    }

    auto map_keys_or_values_with_buckets_state = std::make_shared<DeserializeBinaryBulkStateMapKeysOrValuesWithBuckets>();

    /// Read the bucket count from the shared buckets info stream.
    /// Uses the same `deserializeBucketsInfoStatePrefix` as `SerializationMap`,
    /// so the cached state is reused when both the full Map and a subcolumn are read.
    map_keys_or_values_with_buckets_state->buckets_info_state = SerializationMap::deserializeBucketsInfoStatePrefix(settings, cache);
    const auto * buckets_info_state_concrete = checkAndGetState<SerializationMap::DeserializeBinaryBulkStateBucketsInfo>(map_keys_or_values_with_buckets_state->buckets_info_state);

    /// Initialize bucket index deserialization state.
    /// Only needed for multi-bucket parts; single-bucket parts preserve order trivially.
    if (buckets_info_state_concrete->buckets > 1)
    {
        map_keys_or_values_with_buckets_state->bucket_index_type = getSmallestIndexesType(buckets_info_state_concrete->buckets);
        map_keys_or_values_with_buckets_state->bucket_index_serialization = map_keys_or_values_with_buckets_state->bucket_index_type->getDefaultSerialization();

        settings.path.push_back(Substream::MapBucketIndexes);
        map_keys_or_values_with_buckets_state->has_bucket_index = settings.check_stream_exists_callback && settings.check_stream_exists_callback(settings.path);
        if (map_keys_or_values_with_buckets_state->has_bucket_index)
            map_keys_or_values_with_buckets_state->bucket_index_serialization->deserializeBinaryBulkStatePrefix(settings, map_keys_or_values_with_buckets_state->bucket_index_state, cache);
        settings.path.pop_back();
    }

    /// Initialize nested deserialization state for keys/values in each bucket.
    map_keys_or_values_with_buckets_state->bucket_keys_or_values_states.resize(buckets_info_state_concrete->buckets);
    for (size_t bucket = 0; bucket < buckets_info_state_concrete->buckets; ++bucket)
    {
        settings.path.push_back(SubstreamType::Bucket);
        settings.path.back().bucket = bucket;
        keys_or_values_serialization->deserializeBinaryBulkStatePrefix(settings, map_keys_or_values_with_buckets_state->bucket_keys_or_values_states[bucket], cache);
        settings.path.pop_back();
    }

    state = std::move(map_keys_or_values_with_buckets_state);
}

namespace
{

/// Reassembles a single Array(key_type) or Array(value_type) column from per-bucket Array columns.
/// Similar to `collectMapFromBuckets` in SerializationMap but works with a single Array column
/// (keys or values) instead of a full Map(key, value).
void collectMapKeysOrValuesFromBuckets(const VectorWithMemoryTracking<ColumnPtr> & keys_or_values_buckets, IColumn & keys_or_values_column)
{
    if (keys_or_values_buckets.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty list of buckets provided");

    VectorWithMemoryTracking<ColumnPtr> data_buckets(keys_or_values_buckets.size());
    std::vector<const ColumnArray::Offsets *> offsets_buckets(keys_or_values_buckets.size());
    for (size_t bucket = 0; bucket != keys_or_values_buckets.size(); ++bucket)
    {
        const auto & array_column = assert_cast<const ColumnArray &>(*keys_or_values_buckets[bucket]);
        data_buckets[bucket] = array_column.getDataPtr();
        offsets_buckets[bucket] = &array_column.getOffsets();
    }

    auto & array_column = assert_cast<ColumnArray &>(keys_or_values_column);
    auto & data = array_column.getData();
    auto & offsets = array_column.getOffsets();
    size_t num_rows = keys_or_values_buckets[0]->size();
    offsets.reserve(offsets.size() + num_rows);
    data.prepareForSquashing(data_buckets, 1);
    for (size_t i = 0; i != num_rows; ++i)
    {
        for (size_t bucket = 0; bucket != keys_or_values_buckets.size(); ++bucket)
        {
            size_t offset_start = (*offsets_buckets[bucket])[ssize_t(i) - 1];
            size_t offset_end = (*offsets_buckets[bucket])[ssize_t(i)];
            data.insertRangeFrom(*data_buckets[bucket], offset_start, offset_end - offset_start);
        }

        offsets.push_back(data.size());
    }
}

/// Reassembles a single Array(key_type) or Array(value_type) column from per-bucket Array columns,
/// restoring original insertion order using the bucket index array.
template <typename IndexColumn>
void collectMapKeysOrValuesFromBucketsWithOrderImpl(
    const VectorWithMemoryTracking<ColumnPtr> & keys_or_values_buckets,
    const IndexColumn & bucket_index_col,
    IColumn & keys_or_values_column)
{
    if (keys_or_values_buckets.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty list of buckets provided");

    VectorWithMemoryTracking<ColumnPtr> data_buckets(keys_or_values_buckets.size());
    std::vector<const ColumnArray::Offsets *> offsets_buckets(keys_or_values_buckets.size());
    for (size_t bucket = 0; bucket != keys_or_values_buckets.size(); ++bucket)
    {
        const auto & array_column = assert_cast<const ColumnArray &>(*keys_or_values_buckets[bucket]);
        data_buckets[bucket] = array_column.getDataPtr();
        offsets_buckets[bucket] = &array_column.getOffsets();
    }

    auto & array_column = assert_cast<ColumnArray &>(keys_or_values_column);
    auto & data = array_column.getData();
    auto & offsets = array_column.getOffsets();
    size_t num_rows = keys_or_values_buckets[0]->size();
    offsets.reserve(offsets.size() + num_rows);

    const auto & bucket_index_data = bucket_index_col.getData();
    std::vector<size_t> bucket_positions(keys_or_values_buckets.size());
    size_t bucket_index_offset = 0;

    for (size_t i = 0; i != num_rows; ++i)
    {
        size_t total_size = 0;
        for (size_t bucket = 0; bucket < keys_or_values_buckets.size(); ++bucket)
        {
            size_t offset_start = (*offsets_buckets[bucket])[ssize_t(i) - 1];
            size_t offset_end = (*offsets_buckets[bucket])[ssize_t(i)];
            bucket_positions[bucket] = offset_start;
            total_size += offset_end - offset_start;
        }

        for (size_t j = 0; j < total_size; ++j)
        {
            size_t bucket_idx = bucket_index_data[bucket_index_offset++];
            if (bucket_idx >= keys_or_values_buckets.size())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Bucket index {} is out of range, total buckets: {}", bucket_idx, keys_or_values_buckets.size());
            size_t pos = bucket_positions[bucket_idx]++;
            data.insertFrom(*data_buckets[bucket_idx], pos);
        }

        offsets.push_back(data.size());
    }
}

/// Dispatch on the index column type for collectMapKeysOrValuesFromBucketsWithOrderImpl.
void collectMapKeysOrValuesFromBucketsWithOrder(
    const VectorWithMemoryTracking<ColumnPtr> & keys_or_values_buckets,
    const IColumn & bucket_index_column,
    IColumn & keys_or_values_column)
{
    switch (bucket_index_column.getDataType())
    {
        case TypeIndex::UInt8:
            collectMapKeysOrValuesFromBucketsWithOrderImpl(keys_or_values_buckets, static_cast<const ColumnVector<UInt8> &>(bucket_index_column), keys_or_values_column);
            break;
        case TypeIndex::UInt16:
            collectMapKeysOrValuesFromBucketsWithOrderImpl(keys_or_values_buckets, static_cast<const ColumnVector<UInt16> &>(bucket_index_column), keys_or_values_column);
            break;
        case TypeIndex::UInt32:
            collectMapKeysOrValuesFromBucketsWithOrderImpl(keys_or_values_buckets, static_cast<const ColumnVector<UInt32> &>(bucket_index_column), keys_or_values_column);
            break;
        case TypeIndex::UInt64:
            collectMapKeysOrValuesFromBucketsWithOrderImpl(keys_or_values_buckets, static_cast<const ColumnVector<UInt64> &>(bucket_index_column), keys_or_values_column);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected bucket index column type: {}", bucket_index_column.getName());
    }
}

}

void SerializationMapKeysOrValues::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    /// BASIC format has no bucketing, delegate directly.
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        keys_or_values_serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_offset, limit, settings, state, cache);
        return;
    }

    auto * map_keys_or_values_with_buckets_state = checkAndGetState<DeserializeBinaryBulkStateMapKeysOrValuesWithBuckets>(state);
    const auto * buckets_info_state_concrete = checkAndGetState<SerializationMap::DeserializeBinaryBulkStateBucketsInfo>(map_keys_or_values_with_buckets_state->buckets_info_state);

    /// Fast path for a single bucket. Deserialize directly into the column.
    if (buckets_info_state_concrete->buckets == 1)
    {
        settings.path.push_back(Substream::Bucket);
        settings.path.back().bucket = 0;
        keys_or_values_serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_offset, limit, settings, map_keys_or_values_with_buckets_state->bucket_keys_or_values_states[0], cache);
        settings.path.pop_back();
    }
    /// Multiple buckets. Deserialize each bucket, then reassemble into a single Array column.
    /// If bucket index data is available, use it to restore original insertion order;
    /// otherwise fall back to bucket-ascending order (old parts without the index stream).
    else
    {
        /// The `bucket_indexes` stream is a flat array with one entry per key-value pair, so the
        /// number of entries that belong to the first `rows_offset` rows is not known in advance
        /// and those entries cannot be skipped on their own. Read the skipped rows together with
        /// the requested ones, reassemble the whole range in the original order and drop the
        /// prefix afterwards, so that the index stream stays in sync with the bucket streams.
        const bool reorder_with_skipped_rows = map_keys_or_values_with_buckets_state->has_bucket_index && rows_offset != 0;
        const size_t buckets_rows_offset = reorder_with_skipped_rows ? 0 : rows_offset;
        const size_t buckets_limit = reorder_with_skipped_rows ? rows_offset + limit : limit;

        VectorWithMemoryTracking<ColumnPtr> keys_or_values_buckets(buckets_info_state_concrete->buckets);
        for (size_t bucket = 0; bucket != buckets_info_state_concrete->buckets; ++bucket)
        {
            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;
            keys_or_values_buckets[bucket] = column->cloneEmpty();
            keys_or_values_serialization->deserializeBinaryBulkWithMultipleStreams(keys_or_values_buckets[bucket], buckets_rows_offset, buckets_limit, settings, map_keys_or_values_with_buckets_state->bucket_keys_or_values_states[bucket], cache);
            settings.path.pop_back();
        }

        if (map_keys_or_values_with_buckets_state->has_bucket_index)
        {
            /// Compute total key-value pairs from per-bucket offsets.
            size_t total_kv_pairs = 0;
            for (size_t bucket = 0; bucket != buckets_info_state_concrete->buckets; ++bucket)
            {
                const auto & bucket_offsets = assert_cast<const ColumnArray &>(*keys_or_values_buckets[bucket]).getOffsets();
                if (!bucket_offsets.empty())
                    total_kv_pairs += bucket_offsets.back();
            }

            /// Read bucket indexes (flat array, one per key-value pair).
            ColumnPtr bucket_index_column = map_keys_or_values_with_buckets_state->bucket_index_type->createColumn();
            settings.path.push_back(Substream::MapBucketIndexes);
            map_keys_or_values_with_buckets_state->bucket_index_serialization->deserializeBinaryBulkWithMultipleStreams(
                bucket_index_column, 0, total_kv_pairs, settings, map_keys_or_values_with_buckets_state->bucket_index_state, cache);
            settings.path.pop_back();

            if (reorder_with_skipped_rows)
            {
                auto whole_range_column = column->cloneEmpty();
                collectMapKeysOrValuesFromBucketsWithOrder(keys_or_values_buckets, *bucket_index_column, *whole_range_column);
                if (whole_range_column->size() > rows_offset)
                    column->assumeMutable()->insertRangeFrom(*whole_range_column, rows_offset, whole_range_column->size() - rows_offset);
            }
            else
            {
                collectMapKeysOrValuesFromBucketsWithOrder(keys_or_values_buckets, *bucket_index_column, *column->assumeMutable());
            }
        }
        else
        {
            collectMapKeysOrValuesFromBuckets(keys_or_values_buckets, *column->assumeMutable());
        }
    }
}

}
