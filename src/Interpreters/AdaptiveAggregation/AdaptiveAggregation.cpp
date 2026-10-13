#include <algorithm>
#include <functional>
#include <numeric>
#include <optional>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregationImpl.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/HashTablesStatistics.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationLocalFreezes;
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

/// Compares the repeated records' staging cost with the state an ordinary table would retain. Distinct keys
/// are estimated by scaling the hash sample; the record count is known exactly. Counting only sampled
/// occurrences would let one frequent sampled key distort the repetition estimate for the whole stream.
/// Growing set states also retain distinct arguments, even when their group keys repeat. Their estimated
/// payload is added to the per-key state cost. Multiplication by the record count avoids division, and
/// 128-bit products accommodate streams of billions of records without overflow.
bool Aggregator::adaptiveStagingWastes(
    const AdaptiveAggregationProducer & adaptive,
    size_t state_bytes_per_key,
    size_t state_cost_multiplier) const
{
    const auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    const size_t distinct = frozen.getEstimatedStagedKeyCount();
    if (frozen.staged_records < adaptive_thaw_min_staged_records
        || frozen.staged_records * adaptive_thaw_staged_share_inverse < frozen.rows
        || !distinct || frozen.staged_records <= distinct)
        return false;

    const size_t retained_bytes_per_key = std::max(adaptive_staging_min_state_bytes_per_key, state_bytes_per_key);
    const UInt128 state_bytes = static_cast<UInt128>(retained_bytes_per_key) * distinct
        + static_cast<UInt128>(adaptive_state_bytes_per_distinct_input) * frozen.getEstimatedDistinctInputCount();

    /// The fixed argument area has one width for the whole query. Deriving its size here keeps the
    /// record writers' accounting limited to keys, variable arguments and bookkeeping. The layout
    /// counts a shared argument once and excludes arguments stored in their numeric key's bytes.
    UInt128 staged_bytes = frozen.staged_bytes;
    if (!is_simple_count && params.aggregates_size)
        staged_bytes += static_cast<UInt128>(frozen.staged_records) * adaptive_argument_layout->fixed_bytes;

    return static_cast<UInt128>(frozen.staged_records - distinct) * staged_bytes
        > state_bytes * frozen.staged_records * state_cost_multiplier;
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

void Aggregator::freezeAdaptive(AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive) const
{
    std::call_once(adaptive.session->init_flag, [&] { initAdaptiveSession(*adaptive.session); });
    /// A table that was written to disk freezes again with the records and the bins it already has.
    if (!adaptive.partitions)
    {
        /// Share allocation blocks across 16 consecutive buckets, so their memory is released as merge tasks
        /// advance through the bucket order, while retaining the same grouping when pruning reorders buckets.
        const auto & layout = adaptive.session->layout;
        adaptive.partitions = std::make_unique<PartitionedRecordBuffer>(layout.numPartitions(), 16 * layout.partitionsPerBucket());
        adaptive.session->frozen_producers.fetch_add(1, std::memory_order_relaxed);
    }
    if (adaptive.session->top_k_pruning && !adaptive.count_bins)
        adaptive.count_bins = std::make_unique<UInt16[]>(adaptive_count_bins);
    const size_t keys = result.sizeWithoutOverflowRow();
    /// A zero key-count threshold or byte pressure can freeze a table before its first group is inserted.
    /// Without a measured per-key allocation cost, the aggregate state size supplies a lower bound
    /// that excludes hash-table and arena overhead.
    adaptive.freeze(keys ? result.allocatedBytes() / keys : total_size_of_aggregate_states);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationLocalFreezes);
    LOG_TRACE(log, "Adaptive aggregation: local table frozen at {} keys", keys);
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
        /// an ordinary local table would retain. A populated table's snapshot at the freeze includes hash-table
        /// capacity and arena overhead, and remains available if the table is flushed before its producer finishes.
        const auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
        if (adaptiveStagingWastes(
                adaptive,
                frozen.estimated_table_bytes_per_key,
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

UInt32 AdaptiveAggregationSession::bucketToMerge(UInt32 claim) const
{
    return top_k_pruning ? top_k_pruning->bucket_order[claim] : claim;
}

void Aggregator::spillFrozenAdaptiveTable(
    AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive, size_t max_temp_file_size) const
{
    /// A frozen table admits no new keys, so it grows only through states that keep growing after the freeze, in the
    /// arena or in heap memory of their own; others would free little for the write. Constant-key blocks can keep
    /// updating retained states even after per-row probes are bypassed.
    if (std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase).absorbed_rows < adaptive_frozen_spill_min_hits)
        return;
    chassert(result.hasData());
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

    const size_t partitions_per_bucket = shared.layout.partitionsPerBucket();
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

    partitions.clear();

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpills);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpilledRecords, spilled_records);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpilledBytes, spilled_bytes);
    LOG_TRACE(log, "Adaptive aggregation: spilled {} staged records ({} bytes)", spilled_records, spilled_bytes);
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
            adaptive,
            total_size_of_aggregate_states,
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
