#include <unordered_set>

#include <Columns/IColumn.h>
#include <Common/Arena.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationStagedRecordsMerged;
    extern const Event AdaptiveAggregationSealedChunks;
    extern const Event AdaptiveAggregationBucketsRetired;
}

namespace DB
{

StagedChunk::AggregatePayload::AggregatePayload() = default;
StagedChunk::AggregatePayload::AggregatePayload(AggregatePayload &&) noexcept = default;
StagedChunk::AggregatePayload & StagedChunk::AggregatePayload::operator=(AggregatePayload &&) noexcept
    = default;
StagedChunk::AggregatePayload::~AggregatePayload() = default;

void Aggregator::prepareStagedChunk(StagedChunk & block) const
{
    auto & payload = std::get<StagedChunk::AggregatePayload>(block.payload);

    auto prep = std::make_unique<StagedChunkPreparation>();
    prep->aggregate_columns.resize(params.aggregates_size);
    prep->instructions.resize(params.aggregates_size + 1);
    prep->instructions[params.aggregates_size].that = nullptr;

    /// The payload columns are already in the drain's form - the seal normalized them at the
    /// gather - so the instructions wire the columns directly and only the combinator
    /// unwrapping remains. Nothing dense is materialized here, and a staged payload is never
    /// sparse.
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        prep->aggregate_columns[i].resize(params.aggregates[i].argument_names.size());
        for (size_t j = 0; j < prep->aggregate_columns[i].size(); ++j)
            prep->aggregate_columns[i][j] = payload.argument_columns[aggregates_positions[i][j]].get();
        buildAggregateFunctionInstruction(
            i, /*has_sparse_arguments=*/false, prep->aggregate_columns, prep->instructions, prep->nested_columns_holder);
    }

    payload.prepared = std::move(prep);
}

void Aggregator::initAdaptiveSession(AggregatedDataVariants & local_result, AdaptiveAggregationSession & shared) const
{
    auto early_drain_variants = std::make_shared<AggregatedDataVariants>();
    early_drain_variants->aggregator = this;
    early_drain_variants->keys_size = params.keys_size;
    early_drain_variants->key_sizes = key_sizes;
    early_drain_variants->init(convertToTwoLevelTypeIfPossible(local_result.type));

    shared.early_drain_variants = std::move(early_drain_variants);
    shared.initialized.store(true, std::memory_order_release);
}

void Aggregator::publishStagedChunk(
    AdaptiveAggregationSession & shared, MutableStagedChunkPtr block) const
{
    chassert(block->wellFormed());

    /// Prepared here, on the publishing thread, so the chunk is immutable once any bucket can
    /// see it.
    if (std::holds_alternative<StagedChunk::AggregatePayload>(block->payload))
        prepareStagedChunk(*block);

    shared.backlog.publish(std::move(block));
}

void AdaptiveAggregationSession::StagedBacklog::publish(const StagedChunkPtr & chunk)
{
    undrained_records.fetch_add(chunk->keys.size(), std::memory_order_relaxed);
    registerChunk(chunk);
}

void AdaptiveAggregationSession::StagedBacklog::registerChunk(const StagedChunkPtr & chunk)
{
    std::shared_lock registry_lock(registry_mutex);
    for (size_t b = 0; b < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++b)
    {
        if (!chunk->keys.recordsForBucket(b))
            continue;

        auto & bucket = buckets[b];
        std::lock_guard lock(bucket.mutex);
        bucket.backlog.push_back(chunk);
    }
}

void AdaptiveAggregationSession::StagedBacklog::releaseMergedBucket(size_t bucket)
{
    std::shared_lock registry_lock(registry_mutex);
    auto & b = buckets[bucket];
    std::lock_guard lock(b.mutex);
    b.backlog = {};
}

std::vector<StagedChunkPtr> AdaptiveAggregationSession::StagedBacklog::takeAllForPressureDrain()
{
    std::vector<StagedChunkPtr> chunks;
    std::unique_lock registry_lock(registry_mutex);
    /// A chunk is registered with every bucket it has records for, so the swap-out sees it
    /// once per such bucket and keeps the first appearance.
    std::unordered_set<const void *> seen;
    for (auto & bucket : buckets)
    {
        std::vector<StagedChunkPtr> claimed;
        {
            std::lock_guard bucket_lock(bucket.mutex);
            claimed.swap(bucket.backlog);
        }
        for (auto & chunk : claimed)
            if (seen.insert(chunk.get()).second)
                chunks.push_back(std::move(chunk));
    }
    return chunks;
}

void Aggregator::retireAdaptiveMergedBucket(AggregatedDataVariants & dest, AdaptiveAggregationSession & shared, size_t bucket) const
{
    dest.adaptive_merge_bucket_arenas[bucket].reset();
    shared.backlog.releaseMergedBucket(bucket);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationBucketsRetired);
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
void sealValueStagedChunkConcatenated(const std::vector<MutableStagedChunkPtr> & minis, StagedChunk & chunk)
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

void Aggregator::stageChunk(
    AdaptiveAggregationProducer & adaptive, MutableStagedChunkPtr block, size_t estimated_payload_bytes) const
{
    /// Coalescing pays in proportion to how many batches merge into one chunk. A batch of at
    /// least half the seal target could only ever merge with one neighbor, gaining almost
    /// nothing for a full extra copy of its data, so it is enqueued as-is.
    if (estimated_payload_bytes * 2 >= adaptive_seal_target_bytes)
    {
        publishStagedChunk(*adaptive.session, std::move(block));
        return;
    }

    adaptive.pending_chunks.push_back(std::move(block));
    adaptive.pending_staged_bytes += estimated_payload_bytes;

    if (adaptive.pending_staged_bytes >= adaptive_seal_target_bytes)
        sealPendingChunks(adaptive);
}

void Aggregator::flushPendingChunks(AdaptiveAggregationProducer & adaptive) const
{
    if (!adaptive.pending_chunks.empty())
        sealPendingChunks(adaptive);
}

void Aggregator::sealPendingChunks(AdaptiveAggregationProducer & adaptive) const
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;

    auto & minis = adaptive.pending_chunks;
    const size_t num_minis = minis.size();

    if (num_minis == 1)
    {
        publishStagedChunk(*adaptive.session, minis.front());
        minis.clear();
        adaptive.pending_staged_bytes = 0;
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
        if (adaptive.seal_dedup.shouldDedup())
        {
            size_t input_records = 0;
            for (const auto & mini : minis)
                input_records += mini->keys.size();
            sealValueStagedChunkDeduplicated(minis, *chunk);
            adaptive.seal_dedup.record(input_records, chunk->keys.size());
        }
        else
            sealValueStagedChunkConcatenated(minis, *chunk);
    }
    else
    {
        concatenateStagedKeys(keys, minis);

        auto columns_of = [](const StagedChunk & mini) -> const Columns &
        { return std::get<StagedChunk::AggregatePayload>(mini.payload).argument_columns; };

        auto & argument_columns = chunk->payload.emplace<StagedChunk::AggregatePayload>().argument_columns;
        argument_columns.assign(columns_of(*minis.front()).size(), nullptr);
        for (const auto & argument_positions : aggregates_positions)
            for (const auto position : argument_positions)
            {
                if (argument_columns[position])
                    continue;

                /// The seal normalized every batch's payload columns to the dense form the
                /// drain consumes, so the buffered batches always agree at a position and the
                /// coalescing is a plain concatenation.
                VectorWithMemoryTracking<ColumnPtr> sources;
                sources.reserve(num_minis);
                for (const auto & mini : minis)
                    sources.push_back(columns_of(*mini)[position]);

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

    publishStagedChunk(*adaptive.session, std::move(chunk));
    minis.clear();
    adaptive.pending_staged_bytes = 0;
}

/// The flushed variants' sizes are meaningless by the time the external path finishes, so a
/// stored entry keeps its sizes: only the verdict is written, and only when the session staged
/// enough records to trust the thaw sampler. Runs without a measurement leave the entry alone.
void Aggregator::recordAdaptiveStagingVerdict(AdaptiveAggregationSession & shared) const
{
    const auto & stats_params = params.stats_collecting_params;
    if (!stats_params.isCollectionAndUseEnabled())
        return;

    bool measured = false;
    bool repeat_dominated = false;
    {
        std::lock_guard lock(shared.thaw_sample_mutex);
        measured = shared.staged_records >= adaptive_thaw_min_staged_records;
        repeat_dominated = shared.thaw_all.load(std::memory_order_relaxed);
    }
    if (!measured)
        return;

    auto & stats = getHashTablesStatistics<AggregationEntry>();
    AggregationEntry entry{.sum_of_sizes = 0, .median_size = 0, .adaptive_staging_repeat_dominated = repeat_dominated};
    if (const auto prev = stats.getSizeHint(stats_params))
    {
        entry.sum_of_sizes = prev->sum_of_sizes;
        entry.median_size = prev->median_size;
    }
    stats.update(entry, stats_params);
}

}
