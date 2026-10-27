#include <algorithm>
#include <optional>

#include <Common/FailPoint.h>
#include <Common/MemoryTrackerSwitcher.h>
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

namespace FailPoints
{
    extern const char adaptive_aggregation_spill_no_room_to_append[];
}

namespace
{

/// The floor of a producer's writes to disk (see `adaptive_spill_min_bytes`), raised to
/// `min_bytes_before_external_group_by`, as for the tables of the baseline path.
size_t adaptiveSpillMinBytes(const Aggregator::Params & params)
{
    return std::max(
        std::min(adaptive_spill_min_bytes, params.max_bytes_before_external_group_by / (2 * params.max_threads)),
        params.min_bytes_before_external_group_by);
}

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
        || frozen.staged_records <= distinct)
        return false;

    const size_t retained_bytes_per_key = std::max(adaptive_staging_min_state_bytes_per_key, state_bytes_per_key);
    const UInt128 state_bytes = static_cast<UInt128>(retained_bytes_per_key) * distinct
        + static_cast<UInt128>(adaptive_state_bytes_per_distinct_input) * frozen.getEstimatedDistinctInputCount();

    return static_cast<UInt128>(frozen.staged_records - distinct) * frozen.staged_bytes
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
    /// - `staged_bytes` grows by the bytes of the batch's records as the buffer holds them, plus
    ///   `adaptive_thaw_record_cost` per record for the lookup of its group at the drain. A column read
    ///   by several aggregates is staged once, so it is counted once; an argument read from its numeric
    ///   key's bytes takes none of its own; a count record carries the combined multiplicity instead of
    ///   arguments.
    ///   Variable-width arguments count in full because staging such a value pays real work
    ///   at every step. The record copies it out of the block, the record pins that memory
    ///   until the merge drains it, and updating the aggregate state from it copies the value
    ///   once more (a string min keeps its own copy of the winning value). A repeated key pays
    ///   all of that on every occurrence, where an unfrozen table would have paid a single
    ///   in-place state update, so each repeat of a heavy value is genuine waste.
    ///   Fixed-width arguments also occupy memory until the drain, and rebuilding their dense
    ///   columns copies them again before the ordinary batch executor consumes them. A repeated
    ///   key pays these copies on each occurrence, while an ordinary table updates its states in
    ///   place. The comparison uses repetition within this producer,
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
        auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
        frozen.staged_records += total;
        frozen.staged_bytes += staged.record_bytes + total * adaptive_thaw_record_cost;
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
    if (tmp_data)
    {
        /// An eighth of the threshold over the three buffers of each of the streams, one per partition. The memory
        /// scheduler can ask for a spill without a threshold, and then the streams take the floor.
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
    AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive, size_t rows, bool over_external_threshold) const
{
    /// Over the external-aggregation threshold a producer writes its staged records to disk, whatever its phase: one
    /// back on the baseline path keeps the records it staged while frozen, which flushing its own table cannot free.
    if (over_external_threshold && adaptiveStagingWorthSpilling(adaptive))
        spillAdaptivePartitions(*adaptive.session, *adaptive.partitions);

    /// A frozen producer whose staged stream repeats its misses thaws for good, and the baseline's checks apply.
    if (adaptive.isFrozen() && adaptiveStagingRepeats(adaptive))
        thawAdaptive(result, adaptive);
    if (adaptive.isBaseline())
        return false;

    /// In place of the baseline's two-level conversion, a learning table freezes at its own bounds: from then on it
    /// only updates the keys it already holds, so it stays single-level and bounded, and the frozen kernel pairs it
    /// with its two-level twin.
    if (adaptive.isLearning())
    {
        std::get<AdaptiveAggregationProducer::LearningState>(adaptive.phase).rows += rows;
        if (adaptiveFreezeBoundReached(result))
            freezeAdaptive(result, adaptive);
    }
    return true;
}

bool Aggregator::adaptiveLearningSlices(const AdaptiveAggregationProducer & adaptive) const
{
    /// `<=` keeps the first block sliced even under a zero threshold, where the slicer freezes the table at its
    /// first row.
    return std::get<AdaptiveAggregationProducer::LearningState>(adaptive.phase).rows
        <= adaptive_learning_slicing_row_multiple * params.adaptive_aggregator_freeze_threshold;
}

bool Aggregator::adaptiveFreezeBoundReached(const AggregatedDataVariants & result) const
{
    /// The byte twin of the key-count bound is the table's own tracked memory: its hash-table buffer, its arenas and
    /// the heap memory that its states own, such as the sets of `uniqExact`, which a few groups can grow while the key
    /// count stays low. It is checked between blocks, so a byte-triggered freeze lands on a block boundary; the
    /// mid-block freeze crossing checks only the key count. The memory of the whole aggregation is no bound: it sums
    /// the allocations of every thread, including the blocks they are processing, so the growth of some tables would
    /// freeze the others.
    const bool freeze_bytes_reached = params.adaptive_aggregator_freeze_threshold_bytes
        && result.memoryUsage() >= params.adaptive_aggregator_freeze_threshold_bytes;
    return (result.sizeWithoutOverflowRow() >= params.adaptive_aggregator_freeze_threshold || freeze_bytes_reached)
        && result.isConvertibleToTwoLevel();
}

bool Aggregator::adaptiveStagingWorthSpilling(const AdaptiveAggregationProducer & adaptive) const
{
    /// A producer holding few staged records leaves them, so the producers that just spilled do not write a trickle of
    /// new records on every block while the query stays over the threshold.
    return adaptive.partitions && adaptive.partitions->allocatedChunkBytes() >= adaptiveSpillMinBytes(params);
}

bool Aggregator::adaptiveTableWorthSpilling(
    const AggregatedDataVariants & result, const AdaptiveAggregationProducer & adaptive) const
{
    const size_t min_bytes = adaptiveSpillMinBytes(params);

    /// A learning table is written once its tracked memory reaches the floor, so a table that was just written and
    /// learns its keys again from empty is not written on every block while the query stays over the threshold.
    if (adaptive.isLearning())
        return result.memoryUsage() >= min_bytes && result.hasData();

    /// A frozen table admits no new keys, so it grows only through states that keep growing after the freeze. A table
    /// that did not grow is never written: it would learn its keys again up to the same bounds, so the write frees
    /// nothing. The growth must pass the floor, so a table that was just written, learned its keys again and froze
    /// again is not written on every block while the query stays over the threshold. The growth shows in the table's
    /// tracked memory, which counts the heap memory that the states own and the chunks that the arenas take. The arenas
    /// take whole chunks of doubling size, so a table that froze just after taking a large chunk fills it without
    /// growing its tracked memory: the bytes that the states took in the arenas count as growth as well.
    const auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    const size_t used_arena_bytes = result.usedArenaBytes();
    /// Nothing frees what the arenas held at the freeze: the frozen kernel builds its lookup keys in a scratch arena.
    chassert(used_arena_bytes >= frozen.used_arena_bytes_at_freeze);
    return result.memoryUsage() > frozen.memory_usage_at_freeze + min_bytes
        || used_arena_bytes - frozen.used_arena_bytes_at_freeze > min_bytes;
}

void Aggregator::freezeAdaptive(AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive) const
{
    freezeWaitingTopKHeap(result);

    /// A table that was written to disk freezes again with the records and the bins it already has.
    if (!adaptive.partitions)
    {
        const auto & layout = adaptive.session->layout;
        adaptive.partitions = std::make_unique<PartitionedRecordBuffer>(
            layout.numPartitions(), adaptive_staging_block_buckets * layout.partitionsPerBucket(), layout.firstChunkBytes());
    }
    if (params.adaptiveTopKPrunes() && !adaptive.count_bins.isAllocated())
        adaptive.count_bins.allocate();
    const size_t keys = result.sizeWithoutOverflowRow();
    /// A zero key-count threshold or byte pressure can freeze a table before its first group is inserted.
    /// Without a measured per-key allocation cost, the aggregate state size supplies a lower bound
    /// that excludes hash-table and arena overhead.
    adaptive.freeze(
        result.usedArenaBytes(), result.memoryUsage(), keys ? result.allocatedBytes() / keys : total_size_of_aggregate_states);
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
        shared.spillable_handed_over_bytes.fetch_add(adaptive.partitions->allocatedChunkBytes(), std::memory_order_relaxed);
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

void Aggregator::spillAdaptiveTable(
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
    if (adaptive.isFrozen())
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationFrozenTableSpills);
    adaptive.learnAgain();
    LOG_TRACE(log, "Adaptive aggregation: wrote the local table of {} keys to disk", keys);
}

size_t Aggregator::spillAdaptiveProducer(
    AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive, bool no_more_keys, size_t at_least_bytes) const
{
    std::optional<MemoryTrackerSwitcher> memory_tracker_switcher;
    MemoryTracker * aggregator_tracker = switchToOwnTracker(result, memory_tracker_switcher);

    size_t released = 0;
    if (const size_t staged_bytes = adaptive.stagedBytes())
    {
        /// The staging runs under the aggregator's tracker (see `executeOnBlock`), and so does the release of its records.
        std::optional<MemoryTrackerSwitcher> aggregator_scope;
        if (aggregator_tracker)
            aggregator_scope.emplace(aggregator_tracker);
        spillAdaptivePartitions(*adaptive.session, *adaptive.partitions);
        released = staged_bytes;
    }
    if (released >= at_least_bytes)
        return released;
    if (adaptive.isBaseline())
        return released + spill(result, no_more_keys);
    if (!result.hasData())
        return released;

    const size_t before = result.memoryUsage();
    spillAdaptiveTable(result, adaptive, before + params.min_free_disk_space);
    const size_t after = result.memoryUsage();
    return released + (before > after ? before - after : 0);
}

size_t Aggregator::spillHandedOverStagedRecords(AdaptiveAggregationSession & shared) const
{
    /// The records were staged under the aggregator's tracker (see `executeOnBlock`), and their release is charged to
    /// it as well.
    std::optional<MemoryTrackerSwitcher> aggregator_scope;
    if (MemoryTracker * aggregator_tracker = aggregatorTracker())
        aggregator_scope.emplace(aggregator_tracker);

    std::lock_guard lock(shared.producer_buffers_mutex);
    const size_t spillable_bytes = shared.spillable_handed_over_bytes.load(std::memory_order_relaxed);
    if (!spillable_bytes)
        return 0;

    size_t released = 0;
    for (const auto & partitions : shared.producer_buffers)
    {
        if (const size_t staged_bytes = partitions->allocatedChunkBytes())
        {
            spillAdaptivePartitions(shared, *partitions);
            released += staged_bytes;
        }
    }
    /// Every hand-over added the bytes of its buffer, and a spill writes out every buffer.
    chassert(released == spillable_bytes);
    shared.spillable_handed_over_bytes.store(0, std::memory_order_relaxed);
    return released;
}

void AdaptiveAggregationSession::endHandedOverSpills()
{
    std::lock_guard lock(producer_buffers_mutex);
    spillable_handed_over_bytes.store(0, std::memory_order_relaxed);
}

void Aggregator::spillAdaptivePartitions(AdaptiveAggregationSession & shared, PartitionedRecordBuffer & partitions) const
{
    if (!shared.spill_scope)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot write to temporary file because temporary file is not initialized");

    /// Every write of this spill goes to a disk that had room for the rest of the spill and the minimum free space, as
    /// the file of an ordinary spill does. A stream created by this spill goes to a disk of the temporary volume with
    /// that room. The appends to existing streams check each disk once, at the first append to it, and that check
    /// covers the later appends to the disk. The partitions written before already took their space on disk, so they
    /// are not counted again.
    size_t unwritten_bytes = partitions.allocatedChunkBytes();
    TemporaryAppendSpaceCheck space_check;

    partitions.finishAppending();

    /// The buffers spilled together start at different partitions, so they do not queue on the same stream's lock
    /// partition after partition.
    const size_t num_partitions = shared.layout.numPartitions();
    const size_t first_partition = (reinterpret_cast<uintptr_t>(&partitions) >> 6) % num_partitions;
    size_t spilled_records = 0;
    size_t spilled_bytes = 0;
    for (size_t i = 0; i < num_partitions; ++i)
    {
        const size_t partition = (first_partition + i) % num_partitions;
        const UInt64 records = partitions.recordsOf(partition);
        if (!records)
            continue;

        const UInt64 partition_bytes = shared.spilled_partitions[partition].append(
            shared.spill_scope, partitions, partition, unwritten_bytes + params.min_free_disk_space, space_check);
        unwritten_bytes -= std::min<size_t>(unwritten_bytes, partition_bytes);
        spilled_bytes += partition_bytes;
        partitions.releasePartition(partition);
        spilled_records += records;
    }

    partitions.clear();

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpills);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpilledRecords, spilled_records);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpilledBytes, spilled_bytes);
    LOG_TRACE(log, "Adaptive aggregation: spilled {} staged records ({} bytes)", spilled_records, spilled_bytes);
}

UInt64 AdaptiveAggregationSession::SpilledPartition::append(
    const TemporaryDataOnDiskScopePtr & scope,
    const PartitionedRecordBuffer & partitions,
    size_t partition,
    size_t reserve_size,
    TemporaryAppendSpaceCheck & space_check)
{
    UInt64 bytes = 0;
    partitions.forEachChunk(partition, [&](std::string_view chunk) { bytes += chunk.size(); });

    std::lock_guard lock(mutex);
    if (!stream)
    {
        stream = std::make_unique<TemporaryDataBuffer>(scope, reserve_size);
    }
    else
    {
        /// Stands for a disk that has no room left for the appends of the later spills.
        fiu_do_on(FailPoints::adaptive_aggregation_spill_no_room_to_append,
        {
            reserve_size = std::numeric_limits<size_t>::max();
        });
        space_check.beforeAppend(*stream, reserve_size);
    }
    writeBinaryLittleEndian(bytes, *stream);
    partitions.forEachChunk(partition, [&](std::string_view chunk) { stream->write(chunk.data(), chunk.size()); });
    record_count += partitions.recordsOf(partition);
    return bytes;
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
