#include <algorithm>

#include <Interpreters/AdaptiveAggregationStagingImpl.h>
#include <Processors/Merges/Algorithms/PartitionedChunkCoalescing.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationStagedRecordsMerged;
    extern const Event AdaptiveAggregationSealedChunks;
}

namespace DB
{

size_t StagedChunk::byteSize() const
{
    size_t bytes = keys.key_bytes.size() + keys.key_offsets.size() * sizeof(UInt64) + keys.routing_hashes.size() * sizeof(UInt64);
    if (const auto * counts = std::get_if<CountPayload>(&payload))
        bytes += counts->multiplicities.size() * sizeof(UInt32);
    else
        for (const auto & column : std::get<AggregatePayload>(payload).argument_columns)
            if (column)
                bytes += column->byteSize();
    return bytes;
}

size_t StagedChunk::allocatedBytes() const
{
    size_t bytes = keys.key_bytes.allocated_bytes() + keys.key_offsets.allocated_bytes() + keys.routing_hashes.allocated_bytes();
    if (const auto * counts = std::get_if<CountPayload>(&payload))
        bytes += counts->multiplicities.allocated_bytes();
    else
        for (const auto & column : std::get<AggregatePayload>(payload).argument_columns)
            if (column)
                bytes += column->allocatedBytes();
    return bytes;
}

MutableStagedChunkPtr StagedChunk::cut(size_t start, size_t length) const
{
    chassert(start <= keys.size() && length <= keys.size() - start);
    const auto & source_keys = keys;
    const size_t end = start + length;

    /// Slices have their final size. Exact reservations avoid growth headroom that would
    /// inflate their allocation-based pressure estimates beyond the chosen range size.
    auto piece = std::make_shared<StagedChunk>();
    auto & result_keys = piece->keys;
    result_keys.fixed_key_size = source_keys.fixed_key_size;
    result_keys.routing_hashes.reserve_exact(length);
    result_keys.routing_hashes.insert(source_keys.routing_hashes.begin() + start, source_keys.routing_hashes.begin() + end);

    const size_t byte_begin = source_keys.keyByteOffsetAt(start);
    const size_t byte_end = source_keys.keyByteOffsetAt(end);
    result_keys.key_bytes.reserve_exact(byte_end - byte_begin);
    result_keys.key_bytes.insert(source_keys.key_bytes.begin() + byte_begin, source_keys.key_bytes.begin() + byte_end);
    if (!source_keys.fixed_key_size)
    {
        result_keys.key_offsets.reserve_exact(length + 1);
        for (size_t i = start; i <= end; ++i)
            result_keys.key_offsets.push_back(source_keys.key_offsets[i] - byte_begin);
    }

    /// Clamping preserves intersections with each bucket, including cuts inside a bucket.
    for (size_t b = 0; b <= ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++b)
        result_keys.bucket_offsets[b] = static_cast<UInt32>(std::clamp<size_t>(source_keys.bucket_offsets[b], start, end) - start);

    if (const auto * counts = std::get_if<CountPayload>(&payload))
    {
        auto & multiplicities = piece->payload.emplace<CountPayload>().multiplicities;
        multiplicities.reserve_exact(length);
        multiplicities.insert(counts->multiplicities.begin() + start, counts->multiplicities.begin() + end);
    }
    else
    {
        const auto & columns = std::get<AggregatePayload>(payload).argument_columns;
        auto & argument_columns = piece->payload.emplace<AggregatePayload>().argument_columns;
        argument_columns.reserve(columns.size());
        for (const auto & column : columns)
            argument_columns.push_back(column ? column->cut(start, length) : nullptr);
    }
    return piece;
}

bool StagedChunk::isWellFormed() const
{
    const size_t records = keys.size();
    if (keys.bucket_offsets.front() != 0 || keys.bucket_offsets.back() != records
        || !std::is_sorted(keys.bucket_offsets.begin(), keys.bucket_offsets.end()))
        return false;
    if (keys.fixed_key_size)
    {
        if (!keys.key_offsets.empty() || keys.key_bytes.size() != records * keys.fixed_key_size)
            return false;
    }
    else
    {
        if (keys.key_offsets.size() != records + 1 || keys.key_offsets.front() != 0
            || keys.key_offsets.back() != keys.key_bytes.size()
            || !std::is_sorted(keys.key_offsets.begin(), keys.key_offsets.end()))
            return false;
    }
    if (const auto * counts = std::get_if<CountPayload>(&payload))
        return counts->multiplicities.size() == records;
    for (const auto & column : std::get<AggregatePayload>(payload).argument_columns)
        if (column && column->size() != records)
            return false;
    return true;
}

void StagedChunkConverter::clearMisses()
{
    miss_source_rows.clear();
    miss_hashes.clear();
    miss_buckets.clear();
    miss_key_sizes.clear();
    miss_multiplicities.clear();
}

namespace
{

auto stagedOffsets(const std::vector<MutableStagedChunkPtr> & minis)
{
    return [&minis](size_t source, size_t partition) { return minis[source]->keys.bucket_offsets[partition]; };
}

/// Concatenates the minis' bucket-grouped keys into `keys`: bucket b's records are the
/// concatenation of the minis' b-slices in buffer order. A caller's payload concatenation must
/// walk the same (bucket, mini) order, so a record keeps one position across the key, hash,
/// and payload arrays.
void concatenateStagedKeys(StagedChunk::StagedKeys & keys, const std::vector<MutableStagedChunkPtr> & minis)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;

    size_t total = 0;
    for (size_t b = 0; b < num_buckets; ++b)
    {
        keys.bucket_offsets[b] = static_cast<UInt32>(total);
        for (const auto & mini : minis)
            total += mini->keys.recordsForBucket(b);
    }
    keys.bucket_offsets[num_buckets] = static_cast<UInt32>(total);

    UInt64 total_key_bytes = 0;
    for (const auto & mini : minis)
        total_key_bytes += mini->keys.key_bytes.size();

    keys.routing_hashes.resize_exact(total);
    forEachPartitionedChunkRange(
        num_buckets, minis.size(), stagedOffsets(minis),
        [&](size_t source, size_t begin, size_t length, size_t destination)
        {
            memcpy(&keys.routing_hashes[destination], &minis[source]->keys.routing_hashes[begin], length * sizeof(UInt64));
        });

    keys.fixed_key_size = minis.front()->keys.fixed_key_size;
    if (!keys.fixed_key_size)
        keys.key_offsets.resize_exact(total + 1);
    keys.key_bytes.resize_exact(total_key_bytes);
    {
        UInt64 byte_pos = 0;
        forEachPartitionedChunkRange(
            num_buckets, minis.size(), stagedOffsets(minis),
            [&](size_t source, size_t begin, size_t length, size_t destination)
            {
                const auto & mini = minis[source];
                const UInt64 src_begin = mini->keys.keyByteOffsetAt(begin);
                const UInt64 slice_bytes = mini->keys.keyByteOffsetAt(begin + length) - src_begin;
                memcpy(keys.key_bytes.data() + byte_pos, mini->keys.key_bytes.data() + src_begin, slice_bytes);
                if (!keys.fixed_key_size)
                    for (size_t j = 0; j < length; ++j)
                        keys.key_offsets[destination + j] = byte_pos + (mini->keys.key_offsets[begin + j] - src_begin);
                byte_pos += slice_bytes;
            });
        if (!keys.fixed_key_size)
            keys.key_offsets[total] = byte_pos;
    }
}

/// Concatenates count payloads while coalescing deduplication is bypassed. Duplicate records
/// retain their multiplicities and are combined when their keys are emplaced during draining.
void concatenateCountChunks(const std::vector<MutableStagedChunkPtr> & minis, StagedChunk & chunk)
{
    concatenateStagedKeys(chunk.keys, minis);

    auto & multiplicities = chunk.payload.emplace<StagedChunk::CountPayload>().multiplicities;
    multiplicities.resize_exact(chunk.keys.size());
    forEachPartitionedChunkRange(
        ADAPTIVE_AGGREGATION_NUM_BUCKETS, minis.size(), stagedOffsets(minis),
        [&](size_t source, size_t begin, size_t length, size_t destination)
        {
            const auto & mini_multiplicities = std::get<StagedChunk::CountPayload>(minis[source]->payload).multiplicities;
            memcpy(&multiplicities[destination], &mini_multiplicities[begin], length * sizeof(UInt32));
        });
}

}

MutableStagedChunkPtr StagedChunkConverter::stage(MutableStagedChunkPtr chunk)
{
    const size_t chunk_bytes = chunk->byteSize();
    /// Coalescing pays in proportion to how many batches merge into one chunk. A batch of at
    /// least half the coalescing target gains little from another full copy, so it is returned
    /// directly for preparation and admission.
    if (chunk_bytes * 2 >= adaptive_coalescing_target_bytes)
        return chunk;

    pending_chunks.push_back(std::move(chunk));
    pending_staged_bytes += chunk_bytes;

    if (pending_staged_bytes >= adaptive_coalescing_target_bytes)
        return flush();
    return {};
}

MutableStagedChunkPtr StagedChunkConverter::flush()
{
    if (pending_chunks.empty())
        return {};

    auto & minis = pending_chunks;
    const size_t num_minis = minis.size();

    if (num_minis == 1)
    {
        auto chunk = std::move(minis.front());
        minis.clear();
        pending_staged_bytes = 0;
        return chunk;
    }

    auto chunk = std::make_shared<StagedChunk>();
    auto & keys = chunk->keys;
    const bool counts_only = minis.front()->countsOnly();
    size_t input_records = 0;
    for (const auto & mini : minis)
    {
        input_records += mini->keys.size();
        chassert(mini->countsOnly() == counts_only);
        chassert(mini->keys.fixed_key_size == minis.front()->keys.fixed_key_size);
    }

    if (counts_only)
    {
        /// Coalescing can merge keys repeated across candidates that per-block deduplication
        /// cannot see. Unproductive passes eventually switch to direct concatenation.
        if (coalescing_dedup.shouldDedup())
        {
            coalesceCountChunksWithDeduplication(minis, *chunk);
            coalescing_dedup.record(input_records, chunk->keys.size());
        }
        else
            concatenateCountChunks(minis, *chunk);
    }
    else
    {
        concatenateStagedKeys(keys, minis);

        auto columns_of = [](const StagedChunk & mini) -> const Columns &
        {
            return std::get<StagedChunk::AggregatePayload>(mini.payload).argument_columns;
        };

        auto & argument_columns = chunk->payload.emplace<StagedChunk::AggregatePayload>().argument_columns;
        const auto & first_columns = columns_of(*minis.front());
        argument_columns.assign(first_columns.size(), nullptr);
        for (size_t position = 0; position < first_columns.size(); ++position)
        {
            if (!first_columns[position])
                continue;

            /// Conversion normalizes each candidate's argument columns, so candidates built
            /// by this converter have matching representations at every populated position.
            VectorWithMemoryTracking<ColumnPtr> sources;
            sources.reserve(num_minis);
            for (const auto & mini : minis)
                sources.push_back(columns_of(*mini)[position]);

            argument_columns[position] = coalescePartitionedColumn(
                sources, ADAPTIVE_AGGREGATION_NUM_BUCKETS, stagedOffsets(minis));
        }
    }

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSealedChunks);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecordsMerged, input_records - keys.size());

    static const auto log = getLogger("StagedChunkConverter");
    LOG_TRACE(
        log,
        "Adaptive aggregation: coalesced {} staged batches into one chunk of {} records",
        num_minis,
        keys.size());

    minis.clear();
    pending_staged_bytes = 0;
    return chunk;
}

void StagedChunkConverter::coalesceCountChunksWithDeduplication(
    const std::vector<MutableStagedChunkPtr> & minis,
    StagedChunk & chunk)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;

    auto multiplicities_of = [](const StagedChunk & mini) -> const PaddedPODArray<UInt32> &
    {
        return std::get<StagedChunk::CountPayload>(mini.payload).multiplicities;
    };

    size_t total = 0;
    UInt64 total_key_bytes = 0;
    for (const auto & mini : minis)
    {
        total += mini->keys.size();
        total_key_bytes += mini->keys.key_bytes.size();
    }

    auto & keys = chunk.keys;
    auto & multiplicities = chunk.payload.emplace<StagedChunk::CountPayload>().multiplicities;
    keys.fixed_key_size = minis.front()->keys.fixed_key_size;
    keys.routing_hashes.resize_exact(total);
    multiplicities.resize_exact(total);
    if (!keys.fixed_key_size)
        keys.key_offsets.resize_exact(total + 1);
    keys.key_bytes.resize_exact(total_key_bytes);

    /// Group each bucket's records by a few hash bits before comparing keys. Repeated keys
    /// fall in the same group, so deduplication only scans that group's surviving records.
    struct StagedRef
    {
        UInt64 hash;
        UInt32 mini;
        UInt32 index;
    };
    std::vector<StagedRef> refs;
    std::vector<StagedRef> grouped;

    size_t out = 0;
    UInt64 byte_pos = 0;
    for (size_t b = 0; b < num_buckets; ++b)
    {
        keys.bucket_offsets[b] = static_cast<UInt32>(out);

        constexpr size_t num_groups = 256;
        std::array<UInt32, num_groups + 1> group_offsets{};

        /// One pass collects the bucket's records and their group histogram together; the
        /// records are then scattered whole into group order, so the dedup pass reads them
        /// sequentially instead of gathering through an index vector.
        refs.clear();
        for (size_t m = 0; m < minis.size(); ++m)
        {
            const auto & mini = *minis[m];
            for (size_t j = mini.keys.bucket_offsets[b]; j < mini.keys.bucket_offsets[b + 1]; ++j)
            {
                refs.push_back({mini.keys.routing_hashes[j], static_cast<UInt32>(m), static_cast<UInt32>(j)});
                ++group_offsets[((mini.keys.routing_hashes[j] >> 10) & 0xFF) + 1];
            }
        }
        if (refs.empty())
            continue;

        for (size_t g = 0; g < num_groups; ++g)
            group_offsets[g + 1] += group_offsets[g];
        std::array<UInt32, num_groups> group_cursor{};
        for (size_t g = 0; g < num_groups; ++g)
            group_cursor[g] = group_offsets[g];
        grouped.resize(refs.size());
        for (const auto & ref : refs)
            grouped[group_cursor[(ref.hash >> 10) & 0xFF]++] = ref;

        for (size_t g = 0; g < num_groups; ++g)
        {
            const size_t group_out_begin = out;
            for (size_t i = group_offsets[g]; i < group_offsets[g + 1]; ++i)
            {
                const auto & ref = grouped[i];
                const auto & mini = *minis[ref.mini];

                /// Batch key bytes live in the minis' padded staged arrays.
                const AdaptiveStagingDetail::KeyBytesRef key{
                    mini.keys.keyBytesAt(ref.index), AdaptiveStagingDetail::ReadablePadding::AtLeast15Bytes};
                AdaptiveStagingDetail::mergeOrAppendStagedCount(
                    keys, multiplicities, ref.hash, key, multiplicities_of(mini)[ref.index], group_out_begin, out, byte_pos);
            }
        }
    }

    keys.bucket_offsets[num_buckets] = static_cast<UInt32>(out);
    if (!keys.fixed_key_size)
    {
        keys.key_offsets[out] = byte_pos;
        keys.key_offsets.resize(out + 1);
    }

    keys.routing_hashes.resize(out);
    multiplicities.resize(out);
    keys.key_bytes.resize(byte_pos);
}

}
