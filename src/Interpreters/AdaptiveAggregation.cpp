#include <algorithm>
#include <numeric>
#include <optional>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/HashTablesStatistics.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationBucketsRetired;
    extern const Event AdaptiveAggregationSpills;
    extern const Event AdaptiveAggregationSpilledRecords;
    extern const Event AdaptiveAggregationSpilledBytes;
    extern const Event AdaptiveAggregationFrozenTableSpills;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Compares the repeated records' staging cost with the state an ordinary table would retain. Distinct keys
/// are estimated by scaling the hash sample; the record count is known exactly. Counting only sampled
/// occurrences would let one frequent sampled key distort the repetition estimate for the whole stream.
/// Growing set states also retain distinct arguments, even when their group keys repeat. Their estimated
/// payload is added to the per-key state cost. Multiplication by the record count avoids division, and
/// 128-bit products accommodate streams of billions of records without overflow.
bool adaptiveStagingWastes(
    const AdaptiveAggregationProducer::FrozenState & frozen,
    size_t state_bytes_per_key,
    size_t state_bytes_per_distinct_input,
    size_t state_cost_multiplier)
{
    const size_t distinct = frozen.getEstimatedStagedKeyCount();
    if (frozen.staged_records < adaptive_thaw_min_staged_records
        || frozen.staged_records * adaptive_thaw_staged_share_inverse < frozen.rows
        || !distinct || frozen.staged_records <= distinct)
        return false;

    const size_t retained_bytes_per_key = std::max(adaptive_staging_min_state_bytes_per_key, state_bytes_per_key);
    const UInt128 state_bytes = static_cast<UInt128>(retained_bytes_per_key) * distinct
        + static_cast<UInt128>(state_bytes_per_distinct_input) * frozen.getEstimatedDistinctInputCount();
    return static_cast<UInt128>(frozen.staged_records - distinct) * frozen.staged_bytes
        > state_bytes * frozen.staged_records * state_cost_multiplier;
}

}

void Aggregator::initAdaptiveSession(AdaptiveAggregationSession & shared) const
{
    if (params.adaptiveTopKPrunes())
        shared.top_k_pruning = std::make_unique<AdaptiveTopKPruning>(params.bucket_top_k);

    if (tmp_data && params.max_bytes_before_external_group_by)
    {
        /// An eighth of the threshold over the three buffers of each of the streams.
        const size_t buffers_share = params.max_bytes_before_external_group_by / (8 * 3 * ADAPTIVE_AGGREGATION_NUM_BUCKETS);
        const size_t buffer_bytes = std::min(tmp_data->getSettings().buffer_size, std::max(buffers_share, adaptive_spill_min_buffer_bytes));
        shared.spill_scope = tmp_data->childScope(tmp_data->getSettings().metrics, buffer_bytes);
    }
    shared.initialized.store(true, std::memory_order_release);
}

void Aggregator::finishAdaptiveProducer(AggregatedDataVariants & local_variants, AdaptiveAggregationProducer & adaptive) const
{
    auto & shared = *adaptive.session;

    /// The bins take the rows of the producer's own table as well, whatever its phase: a producer that never froze
    /// has no records but its table is a source of the merge.
    std::optional<AdaptiveTopKPruning::ProducerBins> bins;
    if (shared.top_k_pruning && !adaptive.count_bins)
        adaptive.count_bins = std::make_unique<UInt16[]>(adaptive_count_bins);

    size_t estimated_merge_work = adaptive.total_staged_records * adaptive_parallel_merge_indices.size();
    if (adaptive.count_bins || !adaptive_parallel_merge_indices.empty())
        estimated_merge_work += collectAdaptiveTableStatistics(local_variants, adaptive.count_bins.get());

    if (shared.top_k_pruning)
    {
        bins.emplace();
        bins->bins = std::move(adaptive.count_bins);
        for (size_t bucket = 0; bucket < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++bucket)
        {
            const UInt16 * bucket_bins = bins->bins.get() + bucket * adaptive_count_bins_per_bucket;
            bins->bucket_maxima[bucket] = *std::max_element(bucket_bins, bucket_bins + adaptive_count_bins_per_bucket);
        }
    }

    /// A producer that finishes frozen counts toward the verdict of the run when its whole staged stream repeated past
    /// the bound of the verdict; one that thawed was counted at its thaw.
    if (adaptive.isFrozen() && adaptiveMayThaw(shared))
    {
        /// Admission compares complete executions, so its state cost includes the hash buffer and arenas
        /// an ordinary local table would retain. The snapshot at the freeze includes hash-table capacity
        /// and arena overhead, and remains available if the table is flushed before its producer finishes.
        const auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
        if (adaptiveStagingWastes(
                frozen,
                frozen.allocated_bytes_per_key,
                adaptive_state_bytes_per_distinct_input,
                /*state_cost_multiplier=*/1))
            shared.repeat_dominated_producers.fetch_add(1, std::memory_order_relaxed);
    }

    /// A producer that never froze staged nothing.
    if (adaptive.partitions)
        adaptive.partitions->finishAppending();

    std::lock_guard lock(shared.producer_buffers_mutex);
    shared.estimated_merge_work += estimated_merge_work;
    ++shared.finished_producers;
    if (adaptive.partitions)
        shared.producer_buffers.push_back(std::move(adaptive.partitions));
    if (bins)
        shared.top_k_pruning->producer_bins.push_back(std::move(*bins));
}

void Aggregator::prepareAdaptiveTopKPruning(AdaptiveAggregationSession & shared, size_t producers) const
{
    auto & pruning = *shared.top_k_pruning;

    /// The bounds hold only if every source table of the merge counted its rows into bins: a producer without the
    /// adaptive context (another aggregator of a mixed projection pipeline) did not.
    if (pruning.producer_bins.size() != producers)
    {
        shared.top_k_pruning.reset();
        return;
    }

    /// A bucket's largest bin is at most the sum of the producers' largest bins in it, which orders the buckets.
    std::array<UInt64, ADAPTIVE_AGGREGATION_NUM_BUCKETS> priorities{};
    for (const auto & producer : pruning.producer_bins)
        for (size_t bucket = 0; bucket < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++bucket)
            priorities[bucket] += producer.bucket_maxima[bucket];
    std::iota(pruning.bucket_order.begin(), pruning.bucket_order.end(), 0);
    std::ranges::stable_sort(pruning.bucket_order, [&](UInt8 lhs, UInt8 rhs) { return priorities[lhs] > priorities[rhs]; });
}

UInt32 Aggregator::adaptiveBucketToMerge(const AdaptiveAggregationSession & shared, UInt32 claim) const
{
    return shared.top_k_pruning ? shared.top_k_pruning->bucket_order[claim] : claim;
}

void Aggregator::spillFrozenAdaptiveTable(
    AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive, size_t max_temp_file_size) const
{
    /// A frozen table admits no new keys, so it grows only through states that keep growing after the freeze, in the
    /// arena or in heap memory of their own; others would free little for the write. A table with the probe bypassed
    /// absorbs no rows at all.
    if (std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase).sampled_hits < adaptive_frozen_spill_min_hits
        || !result.hasData())
        return;
    const bool states_grow = !all_aggregates_has_trivial_destructor
        || std::ranges::any_of(aggregate_functions, [](const IAggregateFunction * function) { return function->allocatesMemoryInArena(); });
    if (!states_grow)
        return;

    /// The table goes to disk as a part of the ordinary external aggregation, which the merge reads next to the staged
    /// records. The producer then learns its keys again in an empty single-level table, which the frozen kernel pairs
    /// with its two-level twin, and freezes again at the same bounds.
    const size_t keys = result.sizeWithoutOverflowRow();
    result.convertToTwoLevel();
    writeToTemporaryFile(result, max_temp_file_size);
    result.resetToSingleLevel();
    adaptive.learnAgain();
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationFrozenTableSpills);
    LOG_TRACE(log, "Adaptive aggregation: wrote the frozen table of {} keys to disk over the external-aggregation threshold", keys);
}

void Aggregator::spillAdaptivePartitions(AdaptiveAggregationProducer & adaptive) const
{
    auto & partitions = *adaptive.partitions;
    auto & shared = *adaptive.session;
    if (!shared.spill_scope)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot write to temporary file because temporary file is not initialized");

    const size_t partitions_per_bucket = partitions.layout().partitionsPerBucket();
    partitions.finishAppending();

    /// The producers that cross the threshold together start at different buckets, so they do not queue on the same
    /// stream's lock bucket after bucket.
    const size_t first_bucket = (reinterpret_cast<uintptr_t>(&adaptive) >> 6) % ADAPTIVE_AGGREGATION_NUM_BUCKETS;
    size_t spilled_records = 0;
    size_t spilled_bytes = 0;
    for (size_t i = 0; i < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++i)
    {
        const size_t bucket = (first_bucket + i) % ADAPTIVE_AGGREGATION_NUM_BUCKETS;
        const size_t first_partition = bucket * partitions_per_bucket;

        bool has_records = false;
        for (size_t sub = 0; sub < partitions_per_bucket; ++sub)
            has_records |= partitions.hasRecords(first_partition + sub);
        if (!has_records)
            continue;

        auto & spilled = shared.spilled_buckets[bucket];
        {
            std::lock_guard lock(spilled.mutex);
            if (!spilled.stream)
                spilled.stream = std::make_unique<TemporaryDataBuffer>(shared.spill_scope);
            auto & out = *spilled.stream;

            for (size_t sub = 0; sub < partitions_per_bucket; ++sub)
            {
                const size_t partition = first_partition + sub;
                if (!partitions.hasRecords(partition))
                    continue;

                UInt64 bytes = 0;
                partitions.forEachChunk(partition, [&](std::string_view chunk) { bytes += chunk.size(); });
                const UInt64 records = partitions.recordsOf(partition);

                writeBinaryLittleEndian(static_cast<UInt32>(sub), out);
                writeBinaryLittleEndian(records, out);
                writeBinaryLittleEndian(bytes, out);
                partitions.forEachChunk(partition, [&](std::string_view chunk) { out.write(chunk.data(), chunk.size()); });

                spilled_records += records;
                spilled_bytes += bytes;
            }
        }

        for (size_t sub = 0; sub < partitions_per_bucket; ++sub)
            partitions.releasePartition(first_partition + sub);
    }

    partitions.resetHeldBytes();

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpills);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpilledRecords, spilled_records);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpilledBytes, spilled_bytes);
    LOG_TRACE(log, "Adaptive aggregation: spilled {} staged records ({} bytes)", spilled_records, spilled_bytes);
}

void Aggregator::retireAdaptiveMergedBucket(AggregatedDataVariants & dest, size_t bucket) const
{
    dest.adaptive_merge_bucket_arenas[bucket].reset();
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationBucketsRetired);
}

AggregatedDataVariantsPtr Aggregator::createAdaptiveExternalMergeDestination() const
{
    auto destination = std::make_shared<AggregatedDataVariants>();
    destination->aggregator = this;
    destination->keys_size = params.keys_size;
    destination->key_sizes = key_sizes;
    destination->init(convertToTwoLevelTypeIfPossible(method_chosen));
    destination->adaptive_merge_bucket_arenas.resize(ADAPTIVE_AGGREGATION_NUM_BUCKETS);
    for (auto & slot : destination->adaptive_merge_bucket_arenas)
        slot = std::make_shared<Arena>();
    return destination;
}

void Aggregator::mergeAdaptiveSourceStates(
    AdaptiveMergeScratch & scratch, const AdaptiveAggregationSession & session, Arena * arena, std::atomic<bool> & is_cancelled) const
{
    auto & places = scratch.places;
    auto & source_places = scratch.source_places;
    const size_t merges = places.size();
    chassert(merges == source_places.size());
    chassert(params.max_threads > 0 && session.finished_producers > 0);

    /// A group larger than a worker's share can hold up the merge. Very large states amortize the pool even
    /// when other groups are busy, while small states do not justify its setup even with idle workers.
    const size_t min_parallel_work = std::clamp(
        session.estimated_merge_work / std::min(params.max_threads, session.finished_producers),
        adaptive_parallel_merge_min_work,
        adaptive_parallel_merge_max_work);

    auto & order = scratch.merge_order;
    bool ordered = false;
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        const IAggregateFunction & function = *aggregate_functions[i];
        const size_t offset = offsets_of_aggregate_states[i];
        if (!function.isAbleToParallelizeMerge() || !function.isParallelizeMergePrepareNeeded())
        {
            function.mergeAndDestroyBatch(places.data(), source_places.data(), merges, offset, *thread_pool, is_cancelled, arena);
            continue;
        }

        /// The merges of one destination are contiguous in this order. A giant set of the merge is the state of a group
        /// most producers hold, merged pairwise in one task: the tail of the whole merge. Merged together, the sets are
        /// converted to two-level in parallel where they are large (`parallelizeMergePrepare`), and each of their
        /// buckets is merged on the pool (`parallelizeMergeMulti`).
        if (!ordered)
        {
            order.resize(merges);
            std::iota(order.begin(), order.end(), 0);
            std::ranges::stable_sort(order, [&](UInt32 lhs, UInt32 rhs) { return places[lhs] < places[rhs]; });
            ordered = true;
        }
        auto & group = scratch.merge_group;
        for (size_t begin = 0; begin < merges;)
        {
            size_t end = begin + 1;
            while (end < merges && places[order[end]] == places[order[begin]])
                ++end;
            size_t group_work = function.getEstimatedMergeWork(places[order[begin]] + offset);
            for (size_t k = begin; k < end; ++k)
                group_work += function.getEstimatedMergeWork(source_places[order[k]] + offset);

            if (group_work <= min_parallel_work)
            {
                for (size_t k = begin; k < end; ++k)
                {
                    function.merge(places[order[k]] + offset, source_places[order[k]] + offset, arena);
                    function.destroy(source_places[order[k]] + offset);
                }
            }
            else
            {
                group.clear();
                group.push_back(places[order[begin]] + offset);
                for (size_t k = begin; k < end; ++k)
                    group.push_back(source_places[order[k]] + offset);
                function.parallelizeMergePrepare(group, *thread_pool, is_cancelled);
                function.parallelizeMergeMulti(group, *thread_pool, is_cancelled, arena);
                for (size_t k = begin; k < end; ++k)
                    function.destroy(source_places[order[k]] + offset);
            }
            begin = end;
        }
    }
}

bool Aggregator::adaptiveMayThaw(const AdaptiveAggregationSession & shared) const
{
    /// Under the top-K pruning the frozen tables pay even for a repetitive stream: the merge skips the units whose
    /// groups cannot reach the top, which a thawed table, a source of every unit, would no longer allow.
    return !params.adaptive_aggregator_disable_thaw && !shared.top_k_pruning;
}

bool Aggregator::adaptiveStagingRepeats(const AdaptiveAggregationProducer & adaptive) const
{
    /// Switching in the middle of a stream retains the staged records as well as the new table. Apply
    /// the same hysteresis to the estimated state cost as the calibrated bounds apply to cheap states.
    return adaptiveMayThaw(*adaptive.session)
        && adaptiveStagingWastes(
            std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase),
            total_size_of_aggregate_states,
            adaptive_state_bytes_per_distinct_input,
            adaptive_thaw_state_cost_multiplier);
}

std::optional<bool> Aggregator::adaptiveStagingVerdict(const AdaptiveAggregationSession & shared) const
{
    /// A run measures the verdict only when some producer froze and the producers could thaw: a run that may not thaw
    /// gathers no evidence, and keeping its tables frozen whatever the repeats is what it asked for. The verdict needs
    /// half of the frozen producers repeat-dominated, so a few threads that repeat on an otherwise healthy stream do
    /// not keep the next runs off the adaptive path.
    const size_t frozen = shared.frozen_producers.load(std::memory_order_relaxed);
    if (!frozen || !adaptiveMayThaw(shared))
        return std::nullopt;
    return 2 * shared.repeat_dominated_producers.load(std::memory_order_relaxed) >= frozen;
}

/// The flushed variants' sizes are meaningless by the time the external path finishes, so a stored entry keeps its
/// sizes: only the verdict is written, and only when the run measured one.
void Aggregator::recordAdaptiveStagingVerdict(const AdaptiveAggregationSession & shared) const
{
    const auto & stats_params = params.stats_collecting_params;
    if (!stats_params.isCollectionAndUseEnabled())
        return;

    const auto repeat_dominated = adaptiveStagingVerdict(shared);
    if (!repeat_dominated)
        return;

    auto & stats = getHashTablesStatistics<AggregationEntry>();
    AggregationEntry entry{.adaptive_staging_repeat_dominated = *repeat_dominated};
    if (const auto prev = stats.getSizeHint(stats_params))
    {
        entry.sum_of_sizes = prev->sum_of_sizes;
        entry.median_size = prev->median_size;
    }
    stats.update(entry, stats_params);
}

}
