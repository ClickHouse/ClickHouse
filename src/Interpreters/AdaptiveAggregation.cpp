#include <unordered_set>

#include <Columns/IColumn.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Interpreters/AdaptiveAggregationExecution.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationBucketsRetired;
    extern const Event AdaptiveAggregationStagedChunkSplits;
    extern const Event AdaptiveAggregationThaws;
}

namespace DB
{

namespace FailPoints
{
extern const char adaptive_aggregation_before_spill_budget_wait[];
}

bool AdaptiveAggregationSession::SpillReservation::reserveOrWait(
    AdaptiveAggregationSession & session_, size_t bytes_, size_t budget_)
{
    std::unique_lock lock(session_.detached_spill_mutex);
    session_.detached_spill_cv.wait(lock, [&]
    {
        const bool ready = fits(session_, bytes_, budget_) || session_.cancelled.load(std::memory_order_relaxed);
        /// The pause holds `detached_spill_mutex`; tests must resume it before calling `cancel`
        /// or changing a reservation, since those operations acquire the same mutex.
        if (!ready)
            fiu_do_on(FailPoints::adaptive_aggregation_before_spill_budget_wait,
                FailPointInjection::notifyPauseAndWaitForResume(FailPoints::adaptive_aggregation_before_spill_budget_wait););
        return ready;
    });
    if (session_.cancelled.load(std::memory_order_relaxed))
        return false;
    chassert(fits(session_, bytes_, budget_));
    grab(session_, bytes_);
    return true;
}

StagedChunk::AggregatePayload::AggregatePayload() = default;
StagedChunk::AggregatePayload::AggregatePayload(AggregatePayload &&) noexcept = default;
StagedChunk::AggregatePayload & StagedChunk::AggregatePayload::operator=(AggregatePayload &&) noexcept
    = default;
StagedChunk::AggregatePayload::~AggregatePayload() = default;

StagedChunkPtr Aggregator::prepareStagedChunk(MutableStagedChunkPtr block) const
{
    if (block->countsOnly())
        return block;

    auto & payload = std::get<StagedChunk::AggregatePayload>(block->payload);

    auto prep = std::make_unique<StagedChunkPreparation>();
    prep->aggregate_columns.resize(params.aggregates_size);
    prep->instructions.resize(params.aggregates_size + 1);
    prep->instructions[params.aggregates_size].that = nullptr;

    /// Conversion normalizes gathered argument columns into the representation consumed by
    /// the drain. Preparation wires those columns directly and unwraps combinators; it does
    /// not materialize another copy of the arguments.
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        prep->aggregate_columns[i].resize(params.aggregates[i].argument_names.size());
        for (size_t j = 0; j < prep->aggregate_columns[i].size(); ++j)
            prep->aggregate_columns[i][j] = payload.argument_columns[aggregates_positions[i][j]].get();
        buildAggregateFunctionInstruction(
            i, /*has_sparse_arguments=*/false, prep->aggregate_columns, prep->instructions, prep->nested_columns_holder);
    }

    payload.prepared = std::move(prep);
    return block;
}

void Aggregator::initAdaptiveSession(AggregatedDataVariants & local_result, AdaptiveAggregationSession & shared) const
{
    auto early_drain_variants = std::make_shared<AggregatedDataVariants>();
    early_drain_variants->aggregator = this;
    early_drain_variants->keys_size = params.keys_size;
    early_drain_variants->key_sizes = key_sizes;
    early_drain_variants->init(convertToTwoLevelTypeIfPossible(local_result.type));

    shared.drain_type = early_drain_variants->type;
    shared.early_drain_variants = std::move(early_drain_variants);
    shared.initialized.store(true, std::memory_order_release);
}

void Aggregator::prepareStagedChunks(
    const AdaptiveAggregationSession & shared, MutableStagedChunkPtr block, std::vector<StagedChunkPtr> & ready_chunks) const
{
    chassert(block->isWellFormed());

    /// Size chunks for the pressure drain before preparing instructions, which borrow their columns.
    /// Splitting preserves each record whole, even when one record exceeds the part estimate.
    auto pieces = splitStagedChunkAtPartBound(shared, *block);
    if (pieces.empty())
    {
        ready_chunks.push_back(prepareStagedChunk(std::move(block)));
        return;
    }

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedChunkSplits);
    LOG_TRACE(
        log,
        "Adaptive aggregation: split a staged chunk of {} records into {} pieces at the part bound",
        block->keys.size(),
        pieces.size());
    block.reset();
    for (auto & piece : pieces)
    {
        chassert(piece->isWellFormed());
        ready_chunks.push_back(prepareStagedChunk(std::move(piece)));
    }
}

void Aggregator::admitStagedChunk(
    AdaptiveAggregationSession & shared, const StagedChunkPtr & chunk, bool use_own_memory_tracker) const
{
    std::optional<MemoryTrackerSwitcher> memory_tracker_switcher;
    if (use_own_memory_tracker)
        memory_tracker_switcher.emplace(memory_tracker.get());
    shared.backlog.publish(chunk);
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

/// Estimates the cost of repeated staging across producers. A sampled hash identifies a key;
/// sampled occurrences divided by distinct hashes estimates its repetition count. The verdict is
/// `(repeat - 1) * bytes_per_record > adaptive_thaw_wasted_bytes_per_key` after enough evidence.
///
/// Sampling precedes the mutex; updating the accumulated evidence and deciding the verdict are
/// serialized. A producer observes `thaw_all` before freezing or at its next post-block check. Records
/// already deferred by a frozen kernel must still reach the drain, regardless of the verdict.
void Aggregator::observeAdaptiveStagedRecords(
    AdaptiveAggregationSession & shared, std::span<const UInt64> hashes, size_t batch_bytes) const
{
    if (!shared.thaw_all.load(std::memory_order_relaxed))
    {
        PaddedPODArray<UInt64> sampled_hashes;
        for (const auto hash : hashes)
            if ((hash & adaptive_thaw_sample_mask) == 0)
                sampled_hashes.push_back(hash);

        std::lock_guard lock(shared.thaw_sample_mutex);
        shared.staged_records += hashes.size();
        shared.staged_bytes += batch_bytes;
        shared.thaw_sampled_records += sampled_hashes.size();
        for (const auto hash : sampled_hashes)
            shared.distinct_sampled_hashes.insert(hash);
        /// Re-checked under the lock: a thread that sampled while another was firing would
        /// otherwise fire a second time. The verdict compares the wasted staged bytes per
        /// distinct key, (repeat - 1) * bytes per record, against the bound. It is rearranged
        /// onto a common denominator so the arithmetic stays integral:
        /// (sampled - distinct) * staged_bytes > bound * distinct * staged_records.
        /// The products are widened to 128 bits: a giant near-unique stream (billions of
        /// staged records times their bytes) overflows 64, and a wrapped product could thaw
        /// a healthy stream.
        const size_t distinct = shared.distinct_sampled_hashes.size();
        if (!shared.thaw_all.load(std::memory_order_relaxed)
            && shared.staged_records >= adaptive_thaw_min_staged_records
            && shared.thaw_sampled_records > distinct
            && static_cast<UInt128>(shared.thaw_sampled_records - distinct) * shared.staged_bytes
                > static_cast<UInt128>(adaptive_thaw_wasted_bytes_per_key) * distinct * shared.staged_records)
        {
            shared.thaw_all.store(true, std::memory_order_relaxed);
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationThaws);
            const double repeat = static_cast<double>(shared.thaw_sampled_records) / static_cast<double>(distinct);
            LOG_TRACE(
                log,
                "Adaptive aggregation: thawing the local tables after {} staged records ({} bytes, repeat factor {:.2f}, {} wasted bytes per key)",
                shared.staged_records,
                shared.staged_bytes,
                repeat,
                static_cast<size_t>((repeat - 1.0) * (static_cast<double>(shared.staged_bytes) / static_cast<double>(shared.staged_records))));
        }
    }
}

void Aggregator::flushPendingChunks(AdaptiveAggregationExecution & execution) const
{
    auto & producer = execution.producer;
    if (auto chunk = producer.converter.flush())
        prepareStagedChunks(*producer.session, std::move(chunk), execution.ready_chunks);
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
