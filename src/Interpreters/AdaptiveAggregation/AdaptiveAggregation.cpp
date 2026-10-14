#include <algorithm>
#include <optional>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregationImpl.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/HashTablesStatistics.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationLocalFreezes;
    extern const Event AdaptiveAggregationThaws;
    extern const Event AdaptiveAggregationStagedRecords;
    extern const Event AdaptiveAggregationStagedBytes;
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

void Aggregator::accountStagedBatch(
    const Columns & columns, AdaptiveAggregationProducer & adaptive, const AdaptiveStagedBytes & staged) const
{
    auto & misses = adaptive.misses;
    const size_t total = misses.size();
    if (!total)
        return;
    const auto * hashes = misses.getHashes().data();

    /// The rows behind the records go to the count bins of the top-K pruning: a count record carries
    /// its combined multiplicity.
    if (adaptive.count_bins.isAllocated())
        for (size_t i = 0; i < total; ++i)
            adaptive.count_bins.add(hashes[i], is_simple_count ? misses.getMultiplicities()[i] : 1);

    adaptive.total_staged_records += total;

    /// The thaw evidence of the thread (see `adaptiveStagingRepeats`). A batch updates:
    ///
    /// - `staged_records` grows by the batch's record count.
    /// - `staged_bytes` grows by the batch's estimated footprint, computed below as
    ///   `batch_bytes`. It counts the key bytes as the kernel staged them, the variable-width
    ///   aggregate arguments at their serialized sizes, and the per-record bookkeeping (the
    ///   eight-byte routing hash, plus eight bytes of key size and padding only for byte-staged
    ///   keys). A column read by several aggregates is staged once, so it is counted once; a
    ///   count batch stages the combined multiplicity instead of arguments.
    ///   Variable-width arguments count in full because staging such a value pays real work
    ///   at every step. The record copies it out of the block, the record pins that memory
    ///   until the merge drains it, and updating the aggregate state from it copies the value
    ///   once more (a string min keeps its own copy of the winning value). A repeated key pays
    ///   all of that on every occurrence, where an unfrozen table would have paid a single
    ///   in-place state update, so each repeat of a heavy value is genuine waste.
    ///   Fixed-width arguments also occupy memory until the drain, and rebuilding their dense
    ///   columns copies them again before the ordinary batch executor consumes them. A repeated
    ///   key pays these copies on each occurrence, while an ordinary table updates its states in
    ///   place. Their fixed area is counted by `adaptiveStagingWastes` from the record count and
    ///   `AdaptiveArgumentLayout::fixed_bytes`. The comparison uses repetition within this producer,
    ///   since equal keys in different producers would also occupy separate ordinary tables. A
    ///   stream of five `UInt64` arguments at global repeat 10 can therefore remain profitable when
    ///   its local streams repeat little.
    /// - The sample receives routing hashes matching `hash & 0xFF == 0`. Each distinct key has the
    ///   same sampling probability, and `distinct_sampled_hashes` collapses its repeats onto one entry.
    ///   For a growing set aggregate, a bounded sketch also counts distinct argument tuples of these
    ///   keys, including the key in their hash because equal arguments in different groups occupy
    ///   separate states.
    if (params.adaptiveMayThaw())
    {
        chassert(!adaptive_state_bytes_per_distinct_input || adaptive_distinct_input_aggregate < aggregates_positions.size());
        size_t batch_bytes = staged.key_bytes + (is_simple_count ? total * sizeof(UInt32) : staged.variable_argument_bytes);
        batch_bytes += total * (sizeof(UInt64) + (staged.keys_as_bytes ? sizeof(UInt64) : 0));

        auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
        frozen.staged_records += total;
        frozen.staged_bytes += batch_bytes;
        for (size_t i = 0; i < total; ++i)
        {
            const auto hash = hashes[i];
            if ((hash & adaptive_thaw_sample_mask) == 0)
            {
                frozen.distinct_sampled_hashes.insert(hash);
                if (adaptive_state_bytes_per_distinct_input)
                {
                    SipHash input_hash;
                    input_hash.update(hash);
                    for (const auto position : aggregates_positions[adaptive_distinct_input_aggregate])
                        columns[position]->updateHashWithValue(misses.getSourceRows()[i], input_hash);
                    frozen.addSampledInputHash(input_hash.get64());
                }
            }
        }
    }

    misses.clear();

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecords, total);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedBytes, staged.key_bytes);
}

AdaptiveAggregationSessionPtr Aggregator::createAdaptiveSession(size_t producers) const
{
    const auto layout = AdaptivePartitionLayout::forProducers(producers, params.max_bytes_before_external_group_by);
    TemporaryDataOnDiskScopePtr spill_scope;
    if (tmp_data && params.max_bytes_before_external_group_by)
    {
        /// An eighth of the threshold over the three buffers of each of the streams, one per partition.
        const size_t buffers_share = params.max_bytes_before_external_group_by / (8 * 3 * layout.numPartitions());
        const size_t buffer_bytes = std::min(tmp_data->getSettings().buffer_size, std::max(buffers_share, adaptive_spill_min_buffer_bytes));
        spill_scope = tmp_data->childScope(tmp_data->getSettings().metrics, buffer_bytes);
    }
    return std::make_shared<AdaptiveAggregationSession>(
        layout,
        std::move(spill_scope),
        params.adaptiveTopKPrunes() ? std::make_unique<AdaptiveTopKPruning>(params.bucket_top_k) : nullptr);
}

bool Aggregator::advanceAdaptiveProducer(
    AggregatedDataVariants & result,
    AdaptiveAggregationProducer & adaptive,
    bool worth_convert_to_two_level,
    Int64 current_memory_usage) const
{
    const bool over_external_threshold = params.max_bytes_before_external_group_by
        && current_memory_usage > static_cast<Int64>(params.max_bytes_before_external_group_by);

    /// Over the external-aggregation threshold a producer writes its staged records to disk, whatever its phase: one
    /// back on the baseline path keeps the records it staged while frozen, which flushing its own table cannot free.
    if (over_external_threshold && adaptiveStagingWorthSpilling(adaptive))
        spillAdaptivePartitions(adaptive);

    /// A frozen producer whose staged stream repeats its misses thaws for good, and the baseline's checks apply.
    if (adaptive.isFrozen() && adaptiveStagingRepeats(adaptive))
        thawAdaptive(result, adaptive);
    if (adaptive.isBaseline())
        return false;

    /// The freeze replaces the baseline's two-level conversion: from now on the local table only updates the keys it
    /// already holds, so it stays single-level and bounded by the freeze bounds, and the frozen kernel pairs it with
    /// its two-level twin.
    if (adaptive.isLearning() && adaptiveFreezeBoundReached(result, worth_convert_to_two_level))
        freezeAdaptive(result, adaptive);
    return true;
}

bool Aggregator::adaptiveFreezeBoundReached(const AggregatedDataVariants & result, bool worth_convert_to_two_level) const
{
    /// The byte twin of the key-count bound is the table's own footprint, its hash-table buffer plus its arenas,
    /// checked between blocks like the baseline's conversion thresholds. The mid-block freeze crossing checks only the
    /// key count, so a byte-triggered freeze lands on a block boundary.
    const bool freeze_bytes_reached = params.adaptive_aggregator_freeze_threshold_bytes
        && result.allocatedBytes() >= params.adaptive_aggregator_freeze_threshold_bytes;
    /// The condition on which the baseline converts its table to two-level freezes a learning table instead, so a
    /// learning table never needs the conversion: the adaptive merge gives it the bucket-parallel merge the conversion
    /// is for. It catches what the two bounds cannot see, a few groups whose states own heap memory (`uniqExact` per
    /// region): the query's tracked memory, the measure of the conversion, counts that memory, and the table's own
    /// footprint does not. Summed over every thread, it can freeze a table before the table reaches its own bounds,
    /// which only stages more of its stream.
    return (result.sizeWithoutOverflowRow() >= params.adaptive_aggregator_freeze_threshold || freeze_bytes_reached
            || worth_convert_to_two_level)
        && result.isConvertibleToTwoLevel();
}

bool Aggregator::adaptiveStagingWorthSpilling(const AdaptiveAggregationProducer & adaptive) const
{
    /// A producer holding few staged records leaves them, so the producers that just spilled do not write a trickle of
    /// new records on every block while the query stays over the threshold.
    return adaptive.partitions
        && adaptive.partitions->allocatedChunkBytes()
            >= std::min(adaptive_spill_min_bytes, params.max_bytes_before_external_group_by / (2 * params.max_threads));
}

bool Aggregator::adaptiveFrozenTableWorthSpilling(const AdaptiveAggregationProducer & adaptive) const
{
    /// A table must absorb enough rows after its freeze before each write, so a table that was just written, learned
    /// its keys again and froze again is not written on every block while the query stays over the threshold. A
    /// frozen table admits no new keys, so it grows only through states that keep growing after the freeze, in the
    /// arena or in heap memory of their own; others would free little for the write. Constant-key blocks can keep
    /// updating retained states even after per-row probes are bypassed.
    if (std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase).absorbed_rows < adaptive_frozen_spill_min_hits)
        return false;
    return !all_aggregates_has_trivial_destructor
        || std::ranges::any_of(aggregate_functions, [](const IAggregateFunction * function) { return function->allocatesMemoryInArena(); });
}

void Aggregator::freezeAdaptive(AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive) const
{
    /// A table that was written to disk freezes again with the records and the bins it already has.
    if (!adaptive.partitions)
    {
        const auto & layout = adaptive.session->layout;
        adaptive.partitions = std::make_unique<PartitionedRecordBuffer>(
            layout.numPartitions(), adaptive_staging_block_buckets * layout.partitionsPerBucket());
    }
    if (params.adaptiveTopKPrunes() && !adaptive.count_bins.isAllocated())
        adaptive.count_bins.allocate();
    const size_t keys = result.sizeWithoutOverflowRow();
    /// A zero key-count threshold or byte pressure can freeze a table before its first group is inserted.
    /// Without a measured per-key allocation cost, the aggregate state size supplies a lower bound
    /// that excludes hash-table and arena overhead.
    adaptive.freeze(keys ? result.allocatedBytes() / keys : total_size_of_aggregate_states);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationLocalFreezes);
    LOG_TRACE(log, "Adaptive aggregation: local table frozen at {} keys", keys);
}

void Aggregator::thawAdaptive(const AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive) const
{
    /// The table resumes ordinary insertion. The records staged so far stay in the producer's partitions, and the
    /// merge drains them.
    const auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    LOG_TRACE(
        log,
        "Adaptive aggregation: thawed the local table at {} keys after {} rows and {} staged records ({} bytes, repeat factor {:.2f})",
        result.sizeWithoutOverflowRow(),
        frozen.rows,
        frozen.staged_records,
        frozen.staged_bytes,
        static_cast<double>(frozen.staged_records)
            / static_cast<double>(frozen.getEstimatedStagedKeyCount()));
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationThaws);
    adaptive.standDown();
}

void Aggregator::finishAdaptiveProducer(AggregatedDataVariants & local_variants, AdaptiveAggregationProducer & adaptive) const
{
    auto & shared = *adaptive.session;
    /// A producer creates its partitions at its first freeze and keeps them through thaws and spills.
    const bool froze = adaptive.partitions != nullptr;

    /// The bins of a producer that froze take the rows of its own table as well, whatever its phase now. A producer
    /// that never froze has no bins and hands its table over instead: the adaptive merge counts it, and the ordinary
    /// merge of an aggregation in which no producer froze does not prune.
    AdaptiveTopKCountBins * count_bins = adaptive.count_bins.isAllocated() ? &adaptive.count_bins : nullptr;

    size_t estimated_merge_work = adaptive.total_staged_records * adaptive_parallel_merge_indices.size();
    if (count_bins || !adaptive_parallel_merge_indices.empty())
        estimated_merge_work += collectAdaptiveTableStatistics(local_variants, count_bins);

    std::optional<AdaptiveTopKPruning::ProducerBins> bins;
    if (count_bins)
        bins = AdaptiveTopKPruning::finalizeProducerCounts(std::move(*count_bins));

    /// A producer that thawed was repeat-dominated. One that finishes frozen is when its whole staged stream repeated
    /// past the bound of the verdict.
    bool repeat_dominated = adaptive.isBaseline();
    if (adaptive.isFrozen() && params.adaptiveMayThaw())
    {
        /// Admission compares complete executions, so its state cost includes the hash buffer and arenas
        /// an ordinary local table would retain. A populated table's snapshot at the freeze includes hash-table
        /// capacity and arena overhead, and remains available if the table is flushed before its producer finishes.
        const auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
        repeat_dominated = adaptiveStagingWastes(
            adaptive,
            frozen.estimated_table_bytes_per_key,
            /*state_cost_multiplier=*/1);
    }

    if (froze)
    {
        adaptive.partitions->finishAppending();
        /// The adaptive merge pairs a bucket's partitions with the bucket tables of its sources, so the table converts
        /// to two-level here, in the producer's own thread. The merge preparation converts the tables of the producers
        /// that never froze.
        if (local_variants.isConvertibleToTwoLevel())
            local_variants.convertToTwoLevel();
    }

    std::lock_guard lock(shared.producer_buffers_mutex);
    shared.estimated_merge_work += estimated_merge_work;
    ++shared.finished_producers;
    if (froze)
    {
        ++shared.frozen_producers;
        shared.repeat_dominated_producers += repeat_dominated;
        shared.producer_buffers.push_back(std::move(adaptive.partitions));
    }
    if (bins)
        shared.top_k_pruning->addProducerBins(std::move(*bins));
    else if (params.adaptiveTopKPrunes())
        shared.top_k_pruning->addMergeCountedTable(local_variants);
}

void Aggregator::prepareAdaptiveTopKPruning(
    AdaptiveAggregationSession & shared, size_t producers, bool external_merge, bool collects_statistics) const
{
    auto & pruning = shared.top_k_pruning;
    if (pruning && (external_merge || collects_statistics || !pruning->prepare(producers)))
        pruning.reset();
}

UInt32 AdaptiveAggregationSession::bucketToMerge(UInt32 claim) const
{
    return top_k_pruning ? top_k_pruning->bucketToMerge(claim) : claim;
}

void Aggregator::spillFrozenAdaptiveTable(
    AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive, size_t max_temp_file_size) const
{
    chassert(result.hasData());

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

    partitions.finishAppending();

    /// The producers that cross the threshold together start at different partitions, so they do not queue on the
    /// same stream's lock partition after partition.
    const size_t num_partitions = shared.layout.numPartitions();
    const size_t first_partition = (reinterpret_cast<uintptr_t>(&adaptive) >> 6) % num_partitions;
    size_t spilled_records = 0;
    size_t spilled_bytes = 0;
    for (size_t i = 0; i < num_partitions; ++i)
    {
        const size_t partition = (first_partition + i) % num_partitions;
        const UInt64 records = partitions.recordsOf(partition);
        if (!records)
            continue;

        UInt64 bytes = 0;
        partitions.forEachChunk(partition, [&](std::string_view chunk) { bytes += chunk.size(); });
        auto & spilled = shared.spilled_partitions[partition];
        {
            std::lock_guard lock(spilled.mutex);
            if (!spilled.stream)
                spilled.stream = std::make_unique<TemporaryDataBuffer>(shared.spill_scope);
            auto & out = *spilled.stream;

            writeBinaryLittleEndian(bytes, out);
            partitions.forEachChunk(partition, [&](std::string_view chunk) { out.write(chunk.data(), chunk.size()); });
            spilled.records += records;
        }
        partitions.releasePartition(partition);
        spilled_records += records;
        spilled_bytes += bytes;
    }

    partitions.clear();

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpills);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpilledRecords, spilled_records);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpilledBytes, spilled_bytes);
    LOG_TRACE(log, "Adaptive aggregation: spilled {} staged records ({} bytes)", spilled_records, spilled_bytes);
}

void AdaptiveAggregationSession::SpilledPartition::readBack(const std::function<bool(const PaddedPODArray<char> &)> & callback)
{
    if (!stream)
        return;

    stream->finishWriting();
    auto in = stream->read();
    PaddedPODArray<char> block;
    while (!in->eof())
    {
        UInt64 bytes = 0;
        readBinaryLittleEndian(bytes, *in);
        block.resize(bytes);
        in->readStrict(block.data(), bytes);
        if (!callback(block))
            break;
    }
    stream.reset();
}

bool Aggregator::adaptiveStagingRepeats(const AdaptiveAggregationProducer & adaptive) const
{
    /// Switching in the middle of a stream retains the staged records as well as the new table. Apply
    /// the same hysteresis to the estimated state cost as the calibrated bounds apply to cheap states.
    return params.adaptiveMayThaw()
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
    if (!shared.frozen_producers || !params.adaptiveMayThaw())
        return std::nullopt;
    return 2 * shared.repeat_dominated_producers >= shared.frozen_producers;
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
