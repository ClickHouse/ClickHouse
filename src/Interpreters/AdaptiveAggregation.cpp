#include <unordered_set>

#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationBucketsRetired;
}

namespace DB
{

void Aggregator::prepareStagedChunk(StagedChunk & block) const
{
    auto & payload = std::get<StagedChunk::AggregatePayload>(block.payload);

    auto prep = std::make_unique<StagedChunkPreparation>();
    prep->aggregate_columns.resize(params.aggregates_size);
    prepareAggregateInstructions(
        payload.argument_columns, prep->aggregate_columns, prep->materialized_columns, prep->instructions, prep->nested_columns_holder);

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

std::unique_ptr<AdaptiveAggregationProducer> Aggregator::createAdaptiveProducer(AdaptiveAggregationSessionPtr session) const
{
    /// The owner (the producing transform) installs the staging sink right after this call:
    /// the destination is transport policy, not the aggregator's.
    return std::make_unique<AdaptiveAggregationProducer>(
        std::move(session), StagedChunkBuilder(aggregates_positions, params.aggregates_size, log));
}

bool AdaptiveAggregationSession::ThawSampler::fold(const PaddedPODArray<UInt64> & hashes)
{
    if (fired())
        return false;

    /// The sample is collected outside the lock (a batch contributes on the order of its
    /// size / 256 entries) and folded into the shared sampler under it.
    PaddedPODArray<UInt64> sampled_hashes;
    for (const auto hash : hashes)
        if ((hash & sample_mask) == 0)
            sampled_hashes.push_back(hash);

    std::lock_guard lock(mutex);
    staged_records += hashes.size();
    sampled_records += sampled_hashes.size();
    for (const auto hash : sampled_hashes)
        distinct_sampled_hashes.insert(hash);

    /// Re-checked under the lock: a thread that sampled while another was firing would
    /// otherwise fire a second time.
    if (fired() || staged_records < min_staged_records
        || sampled_records <= repeat_factor * distinct_sampled_hashes.size())
        return false;

    thaw_all.store(true, std::memory_order_relaxed);
    return true;
}

void StagedChunkBacklogSink::consume(MutableStagedChunkPtr chunk)
{
    aggregator.publishStagedChunk(session, std::move(chunk));
}

void Aggregator::publishStagedChunk(
    AdaptiveAggregationSession & shared, MutableStagedChunkPtr block) const
{
    chassert(block->wellFormed());

    /// The transport prepared the chunk on the producing thread (see the pipeline staging
    /// sink), where the preparation parallelizes across producers, so the chunk is immutable
    /// by the time any bucket can see it.
    chassert(
        !std::holds_alternative<StagedChunk::AggregatePayload>(block->payload)
        || std::get<StagedChunk::AggregatePayload>(block->payload).prepared);

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

void Aggregator::flushStaging(AdaptiveAggregationProducer & adaptive) const
{
    adaptive.staging.flush(*adaptive.staging_sink);
}

/// The flushed variants' sizes are meaningless by the time the external path finishes, so a
/// stored entry keeps its sizes: only the verdict is written, and only when the session staged
/// enough records to trust the thaw sampler. Runs without a measurement leave the entry alone.
void Aggregator::recordAdaptiveStagingVerdict(AdaptiveAggregationSession & shared) const
{
    const auto & stats_params = params.stats_collecting_params;
    if (!stats_params.isCollectionAndUseEnabled())
        return;

    const auto measurement = shared.thaw_sampler.measure();
    if (!measurement.measured)
        return;
    const bool repeat_dominated = measurement.repeat_dominated;

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
