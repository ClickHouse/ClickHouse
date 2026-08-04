#include <DataTypes/Serializations/SerializationMapKeysOrValues.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <Columns/ColumnArray.h>
#include <Common/SipHash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
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

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapKeysOrValuesWithBuckets>(*this);
        new_state->buckets_info_state = buckets_info_state ? buckets_info_state->clone() : nullptr;
        for (size_t bucket = 0; bucket != bucket_keys_or_values_states.size(); ++bucket)
            new_state->bucket_keys_or_values_states[bucket] = bucket_keys_or_values_states[bucket] ? bucket_keys_or_values_states[bucket]->clone() : nullptr;
        return new_state;
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
    else
    {
        VectorWithMemoryTracking<ColumnPtr> keys_or_values_buckets(buckets_info_state_concrete->buckets);
        for (size_t bucket = 0; bucket != buckets_info_state_concrete->buckets; ++bucket)
        {
            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;
            keys_or_values_buckets[bucket] = column->cloneEmpty();
            keys_or_values_serialization->deserializeBinaryBulkWithMultipleStreams(keys_or_values_buckets[bucket], rows_offset, limit, settings, map_keys_or_values_with_buckets_state->bucket_keys_or_values_states[bucket], cache);
            settings.path.pop_back();
        }

        collectMapKeysOrValuesFromBuckets(keys_or_values_buckets, *column->assumeMutable());
    }
}

}
