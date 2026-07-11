#include <DataTypes/Serializations/SerializationMapSize.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <Columns/ColumnsNumber.h>
#include <Common/SipHash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

SerializationMapSize::SerializationMapSize(
    const SerializationPtr & size_serialization_, MergeTreeMapSerializationVersion serialization_version_)
    : size_serialization(size_serialization_)
    , serialization_version(serialization_version_)
{
}

UInt128 SerializationMapSize::getHash(const SerializationPtr & size_serialization_, MergeTreeMapSerializationVersion serialization_version_)
{
    SipHash hash;
    hash.update("MapSize");
    hash.update(size_serialization_->getHash());
    hash.update(static_cast<UInt8>(serialization_version_));
    return hash.get128();
}

SerializationPtr SerializationMapSize::create(const SerializationPtr & size_serialization_, MergeTreeMapSerializationVersion serialization_version_)
{
    if (!size_serialization_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationMapSize(size_serialization_, serialization_version_));
    return ISerialization::pooled(getHash(size_serialization_, serialization_version_), [&] { return new SerializationMapSize(size_serialization_, serialization_version_); });
}

/// Deserialization state for the bucketed Map size subcolumn.
/// Holds the bucket count and per-bucket offset deserialization states.
struct DeserializeBinaryBulkStateMapSizeWithBuckets : public ISerialization::DeserializeBinaryBulkState
{
    /// State from the shared buckets info stream (bucket count and optional statistics).
    ISerialization::DeserializeBinaryBulkStatePtr buckets_info_state;
    /// Per-bucket deserialization state for the array offsets sub-stream.
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> bucket_size_states;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapSizeWithBuckets>(*this);
        new_state->buckets_info_state = buckets_info_state ? buckets_info_state->clone() : nullptr;
        for (size_t bucket = 0; bucket != bucket_size_states.size(); ++bucket)
            new_state->bucket_size_states[bucket] = bucket_size_states[bucket] ? bucket_size_states[bucket]->clone() : nullptr;
        return new_state;
    }
};


void SerializationMapSize::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    auto next_data = SubstreamData(size_serialization)
        .withType(data.type)
        .withColumn(data.column)
        .withSerializationInfo(data.serialization_info)
        .withDeserializeState(data.deserialize_state);

    /// BASIC format has no bucketing, delegate directly.
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        size_serialization->enumerateStreams(settings, callback, next_data);
        return;
    }

    /// The buckets info stream.
    settings.path.push_back(Substream::MapBucketsInfo);
    callback(settings.path);
    settings.path.pop_back();

    /// Need deserialization state to know the bucket count.
    if (!data.deserialize_state)
        return;

    const auto * map_size_with_buckets_deserialize_state = checkAndGetState<DeserializeBinaryBulkStateMapSizeWithBuckets>(data.deserialize_state);
    const auto * buckets_info_state = checkAndGetState<SerializationMap::DeserializeBinaryBulkStateBucketsInfo>(map_size_with_buckets_deserialize_state->buckets_info_state);

    /// Enumerate an offsets sub-stream for each bucket.
    for (size_t bucket = 0; bucket < buckets_info_state->buckets; ++bucket)
    {
        settings.path.push_back(SubstreamType::Bucket);
        settings.path.back().bucket = bucket;
        next_data.deserialize_state =  map_size_with_buckets_deserialize_state->bucket_size_states[bucket];
        size_serialization->enumerateStreams(settings, callback, next_data);
        settings.path.pop_back();
    }
}

/// Serialization methods are not implemented because the size subcolumn is read-only.
/// Writing always goes through `SerializationMap` which handles the full Map column.

void SerializationMapSize::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationMapSize");
}

void SerializationMapSize::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationMapSize");
}

void SerializationMapSize::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationMapSize");
}

void SerializationMapSize::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    /// BASIC format has no bucketing, delegate directly.
    if (serialization_version == MergeTreeMapSerializationVersion::BASIC)
    {
        size_serialization->deserializeBinaryBulkStatePrefix(settings, state, cache);
        return;
    }

    auto map_size_with_buckets_state = std::make_shared<DeserializeBinaryBulkStateMapSizeWithBuckets>();

    /// Read the bucket count from the shared buckets info stream.
    map_size_with_buckets_state->buckets_info_state = SerializationMap::deserializeBucketsInfoStatePrefix(settings, cache);
    const auto * buckets_info_state_concrete = checkAndGetState<SerializationMap::DeserializeBinaryBulkStateBucketsInfo>(map_size_with_buckets_state->buckets_info_state);

    /// Initialize nested deserialization state for offsets in each bucket.
    map_size_with_buckets_state->bucket_size_states.resize(buckets_info_state_concrete->buckets);
    for (size_t bucket = 0; bucket < buckets_info_state_concrete->buckets; ++bucket)
    {
        settings.path.push_back(SubstreamType::Bucket);
        settings.path.back().bucket = bucket;
        size_serialization->deserializeBinaryBulkStatePrefix(settings, map_size_with_buckets_state->bucket_size_states[bucket], cache);
        settings.path.pop_back();
    }

    state = std::move(map_size_with_buckets_state);
}

namespace
{

/// Computes total map size per row by summing per-bucket sizes.
/// Each bucket stores array offsets that were converted to sizes by the nested
/// `SerializationArrayOffsets`. This function sums them element-wise to produce
/// the final UInt64 size column.
void collectMapSizeFromBuckets(const VectorWithMemoryTracking<ColumnPtr> & size_buckets, IColumn & size_column)
{
    if (size_buckets.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty list of buckets provided");

    std::vector<const ColumnUInt64::Container *> data_buckets(size_buckets.size());
    for (size_t bucket = 0; bucket != size_buckets.size(); ++bucket)
        data_buckets[bucket] = &assert_cast<const ColumnUInt64 &>(*size_buckets[bucket]).getData();

    auto & data = assert_cast<ColumnUInt64 &>(size_column).getData();
    size_t num_rows = size_buckets[0]->size();
    data.reserve(data.size() + num_rows);
    for (size_t i = 0; i != num_rows; ++i)
    {
        UInt64 total_size = 0;
        for (auto & data_bucket : data_buckets)
            total_size += (*data_bucket)[i];
        data.push_back(total_size);
    }
}

}

void SerializationMapSize::deserializeBinaryBulkWithMultipleStreams(
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
        size_serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_offset, limit, settings, state, cache);
        return;
    }

    auto * map_size_with_buckets_state = checkAndGetState<DeserializeBinaryBulkStateMapSizeWithBuckets>(state);
    const auto * buckets_info_state_concrete = checkAndGetState<SerializationMap::DeserializeBinaryBulkStateBucketsInfo>(map_size_with_buckets_state->buckets_info_state);

    /// Fast path for a single bucket. The per-bucket size is the total size.
    if (buckets_info_state_concrete->buckets == 1)
    {
        settings.path.push_back(Substream::Bucket);
        settings.path.back().bucket = 0;
        size_serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_offset, limit, settings, map_size_with_buckets_state->bucket_size_states[0], cache);
        settings.path.pop_back();
    }
    /// Multiple buckets. Deserialize sizes from each bucket, then sum them per row.
    else
    {
        VectorWithMemoryTracking<ColumnPtr> size_buckets(buckets_info_state_concrete->buckets);
        for (size_t bucket = 0; bucket != buckets_info_state_concrete->buckets; ++bucket)
        {
            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;
            size_buckets[bucket] = column->cloneEmpty();
            size_serialization->deserializeBinaryBulkWithMultipleStreams(size_buckets[bucket], rows_offset, limit, settings, map_size_with_buckets_state->bucket_size_states[bucket], cache);
            settings.path.pop_back();
        }

        collectMapSizeFromBuckets(size_buckets, *column->assumeMutable());
    }
}

}
