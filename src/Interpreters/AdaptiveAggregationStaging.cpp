#include <algorithm>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/IColumn.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationStagedRecordsMerged;
    extern const Event AdaptiveAggregationSealedChunks;
    extern const Event AdaptiveAggregationSealNormalizations;
}

namespace DB
{

StagedChunk::AggregatePayload::AggregatePayload() = default;
StagedChunk::AggregatePayload::AggregatePayload(AggregatePayload &&) noexcept = default;
StagedChunk::AggregatePayload & StagedChunk::AggregatePayload::operator=(AggregatePayload &&) noexcept = default;
StagedChunk::AggregatePayload::~AggregatePayload() = default;

bool StagedChunk::wellFormed() const
{
    const size_t records = keys.size();
    if (keys.bucket_offsets.back() != records)
        return false;
    if (keys.fixed_key_size)
    {
        if (!keys.key_offsets.empty() || keys.key_bytes.size() != records * keys.fixed_key_size)
            return false;
    }
    else
    {
        if (keys.key_offsets.size() != records + 1 || keys.key_offsets.back() != keys.key_bytes.size())
            return false;
        for (size_t i = 0; i < records; ++i)
            if (keys.key_offsets[i] > keys.key_offsets[i + 1])
                return false;
    }
    for (size_t b = 0; b < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++b)
        if (keys.bucket_offsets[b] > keys.bucket_offsets[b + 1])
            return false;
    if (const auto * counts = std::get_if<CountPayload>(&payload))
        return counts->multiplicities.size() == records;
    for (const auto & column : std::get<AggregatePayload>(payload).argument_columns)
        if (column && column->size() != records)
            return false;
    return true;
}

namespace
{

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
        {
            chassert(mini->countsOnly() == minis.front()->countsOnly());
            total += mini->keys.recordsForBucket(b);
        }
    }
    keys.bucket_offsets[num_buckets] = static_cast<UInt32>(total);

    UInt64 total_key_bytes = 0;
    for (const auto & mini : minis)
        total_key_bytes += mini->keys.key_bytes.size();

    keys.routing_hashes.resize(total);
    {
        size_t pos = 0;
        for (size_t b = 0; b < num_buckets; ++b)
            for (const auto & mini : minis)
            {
                const size_t begin = mini->keys.bucket_offsets[b];
                const size_t length = mini->keys.recordsForBucket(b);
                if (!length)
                    continue;
                memcpy(&keys.routing_hashes[pos], &mini->keys.routing_hashes[begin], length * sizeof(UInt64));
                pos += length;
            }
    }

    keys.fixed_key_size = minis.front()->keys.fixed_key_size;
    if (!keys.fixed_key_size)
        keys.key_offsets.resize(total + 1);
    keys.key_bytes.resize(total_key_bytes);
    {
        size_t pos = 0;
        UInt64 byte_pos = 0;
        for (size_t b = 0; b < num_buckets; ++b)
            for (const auto & mini : minis)
            {
                const size_t begin = mini->keys.bucket_offsets[b];
                const size_t length = mini->keys.recordsForBucket(b);
                if (!length)
                    continue;
                const UInt64 src_begin = mini->keys.keyByteOffsetAt(begin);
                const UInt64 slice_bytes = mini->keys.keyByteOffsetAt(begin + length) - src_begin;
                memcpy(keys.key_bytes.data() + byte_pos, mini->keys.key_bytes.data() + src_begin, slice_bytes);
                if (!keys.fixed_key_size)
                    for (size_t j = 0; j < length; ++j)
                        keys.key_offsets[pos + j] = byte_pos + (mini->keys.key_offsets[begin + j] - src_begin);
                pos += length;
                byte_pos += slice_bytes;
            }
        if (!keys.fixed_key_size)
            keys.key_offsets[total] = byte_pos;
    }
}

/// The bypassed count seal: a straight concatenation with no cross-mini dedup. Duplicate count
/// records are legal - the drain merges them at its emplace - so a stale bypass costs staged
/// memory until the next resample, never results.
void sealCountChunkConcatenated(const std::vector<MutableStagedChunkPtr> & minis, StagedChunk & chunk)
{
    concatenateStagedKeys(chunk.keys, minis);

    auto & multiplicities = chunk.payload.emplace<StagedChunk::CountPayload>().multiplicities;
    multiplicities.resize(chunk.keys.size());
    size_t pos = 0;
    for (size_t b = 0; b < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++b)
        for (const auto & mini : minis)
        {
            const auto & mini_multiplicities = std::get<StagedChunk::CountPayload>(mini->payload).multiplicities;
            const size_t begin = mini->keys.bucket_offsets[b];
            const size_t length = mini->keys.recordsForBucket(b);
            if (!length)
                continue;
            memcpy(&multiplicities[pos], &mini_multiplicities[begin], length * sizeof(UInt32));
            pos += length;
        }
}

}

void StagedChunkBuilder::sealCountChunkDeduplicated(const std::vector<MutableStagedChunkPtr> & minis, StagedChunk & chunk)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;

    auto multiplicities_of = [](const StagedChunk & mini) -> const PaddedPODArray<UInt32> &
    { return std::get<StagedChunk::CountPayload>(mini.payload).multiplicities; };

    size_t total = 0;
    UInt64 total_key_bytes = 0;
    for (const auto & mini : minis)
    {
        chassert(mini->countsOnly());
        total += mini->keys.size();
        total_key_bytes += mini->keys.key_bytes.size();
    }

    auto & keys = chunk.keys;
    auto & multiplicities = chunk.payload.emplace<StagedChunk::CountPayload>().multiplicities;
    keys.fixed_key_size = minis.front()->keys.fixed_key_size;
    keys.routing_hashes.resize(total);
    multiplicities.resize(total);
    if (!keys.fixed_key_size)
        keys.key_offsets.resize(total + 1);
    keys.key_bytes.resize(total_key_bytes);

    /// The publish dedup only sees one block; keys repeating across the buffered batches are
    /// merged here, while the seal copies the records anyway. Same scheme as the publish walk:
    /// group a bucket's records by a few hash bits so a duplicate can only be one of its
    /// group's survivors, then compare within the group.
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
                const KeyBytesRef key{mini.keys.keyBytesAt(ref.index), ReadablePadding::AtLeast15Bytes};
                mergeOrAppendStagedCount(
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

void StagedChunkBuilder::stageBuiltChunk(MutableStagedChunkPtr chunk, size_t estimated_payload_bytes, IStagedChunkSink & sink)
{
    /// Coalescing pays in proportion to how many batches merge into one chunk. A batch of at
    /// least half the seal target could only ever merge with one neighbor, gaining almost
    /// nothing for a full extra copy of its data, so it is emitted as-is.
    if (estimated_payload_bytes * 2 >= adaptive_seal_target_bytes)
    {
        sink.consume(std::move(chunk));
        return;
    }

    pending_chunks.push_back(std::move(chunk));
    pending_staged_bytes += estimated_payload_bytes;

    if (pending_staged_bytes >= adaptive_seal_target_bytes)
        sealPending(sink);
}

void StagedChunkBuilder::flush(IStagedChunkSink & sink)
{
    if (!pending_chunks.empty())
        sealPending(sink);
}

void StagedChunkBuilder::sealPending(IStagedChunkSink & sink)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;

    auto & minis = pending_chunks;
    const size_t num_minis = minis.size();

    if (num_minis == 1)
    {
        sink.consume(minis.front());
        minis.clear();
        pending_staged_bytes = 0;
        return;
    }

    auto chunk = std::make_shared<StagedChunk>();
    auto & keys = chunk->keys;
    const bool counts_only = minis.front()->countsOnly();

    if (counts_only)
    {
        /// The cross-mini dedup merges keys repeating across the buffered batches, which the
        /// per-block publish dedup cannot see. On a distinct stream it merges nothing; the
        /// productivity tracker then degrades the seal to a straight concatenation.
        if (seal_dedup.shouldDedup())
        {
            size_t input_records = 0;
            for (const auto & mini : minis)
                input_records += mini->keys.size();
            sealCountChunkDeduplicated(minis, *chunk);
            seal_dedup.record(input_records, chunk->keys.size());
        }
        else
            sealCountChunkConcatenated(minis, *chunk);
    }
    else
    {
        concatenateStagedKeys(keys, minis);
        const size_t total = keys.size();

        auto columns_of = [](const StagedChunk & mini) -> const Columns &
        { return std::get<StagedChunk::AggregatePayload>(mini.payload).argument_columns; };

        auto & argument_columns = chunk->payload.emplace<StagedChunk::AggregatePayload>().argument_columns;
        argument_columns.assign(columns_of(*minis.front()).size(), nullptr);
        for (const auto & argument_positions : aggregates_positions)
            for (const auto position : argument_positions)
            {
                if (argument_columns[position])
                    continue;

                /// A constant argument stays constant only when every batch agrees on the value.
                /// The values can genuinely differ across the blocks of one stream, and
                /// ColumnConst::insertRangeFrom ignores the source, so a mismatch materializes
                /// every batch's column instead (the same treatment as Squashing).
                bool all_const_equal = isColumnConst(*columns_of(*minis.front())[position]);
                for (size_t m = 1; all_const_equal && m < num_minis; ++m)
                {
                    const auto & column = *columns_of(*minis[m])[position];
                    all_const_equal = isColumnConst(column)
                        && assert_cast<const ColumnConst &>(*columns_of(*minis.front())[position])
                                   .getDataColumn()
                                   .compareAt(0, 0, assert_cast<const ColumnConst &>(column).getDataColumn(), -1)
                            == 0;
                }
                if (all_const_equal)
                {
                    argument_columns[position] = columns_of(*minis.front())[position]->cloneResized(total);
                    continue;
                }

                VectorWithMemoryTracking<ColumnPtr> sources;
                sources.reserve(num_minis);
                for (const auto & mini : minis)
                    sources.push_back(columns_of(*mini)[position]->convertToFullColumnIfConst());

                /// The clone below takes the destination's class from the first source and the two
                /// calls after it downcast every source to that class. Lazy replication is decided
                /// per block, so one position can legitimately mix wrapped and dense columns.
                if (!std::ranges::all_of(sources, [&](const auto & source) { return source->structureEquals(*sources.front()); }))
                {
                    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSealNormalizations);
                    for (auto & source : sources)
                        source = removeSpecialRepresentations(source);
                }

                auto destination = sources.front()->cloneEmpty();
                destination->prepareForSquashing(sources, /* factor */ 1);
                for (size_t b = 0; b < num_buckets; ++b)
                    for (size_t m = 0; m < num_minis; ++m)
                    {
                        const size_t begin = minis[m]->keys.bucket_offsets[b];
                        const size_t length = minis[m]->keys.recordsForBucket(b);
                        if (length)
                            destination->insertRangeFrom(*sources[m], begin, length);
                    }
                argument_columns[position] = std::move(destination);
            }
    }

    size_t batch_records = 0;
    for (const auto & mini : minis)
        batch_records += mini->keys.size();
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSealedChunks);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecordsMerged, batch_records - keys.size());

    LOG_TRACE(
        log,
        "Adaptive aggregation: sealed {} staged batches into one chunk of {} records",
        num_minis,
        keys.size());

    sink.consume(std::move(chunk));
    minis.clear();
    pending_staged_bytes = 0;
}

}
