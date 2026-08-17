#include <shared_mutex>
#include <unordered_set>

#include <Interpreters/AdaptiveAggregationSession.h>

namespace DB
{

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

}
