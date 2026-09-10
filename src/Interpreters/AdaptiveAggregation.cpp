#include <unordered_set>

#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Interpreters/AdaptiveAggregationChunkInfo.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationBucketsRetired;
    extern const Event AdaptiveAggregationStagedChunkSplits;
    extern const Event AdaptiveAggregationThaws;
    extern const Event AdaptiveAggregationStagedBytes;
    extern const Event AdaptiveAggregationStagedRecords;
    extern const Event AdaptiveAggregationStagedRecordsMerged;
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
            prep->aggregate_columns[i][j] = payload.argument_columns[staged_aggregates_positions[i][j]].get();
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

void Aggregator::publishStagedChunk(AdaptiveAggregationSession & shared, MutableStagedChunkPtr block) const
{
    chassert(block->isWellFormed());

    /// Size chunks for the pressure drain before preparing instructions, which borrow their columns.
    /// Splitting preserves each record whole, even when one record exceeds the part estimate.
    auto pieces = splitStagedChunkAtPartBound(shared, *block);
    if (pieces.empty())
    {
        shared.backlog.publish(prepareStagedChunk(std::move(block)));
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
        shared.backlog.publish(prepareStagedChunk(std::move(piece)));
    }
}

ChunkInfo::Ptr AdaptiveAggregationMissesInfo::clone() const
{
    auto copy = std::make_shared<AdaptiveAggregationMissesInfo>(use_own_memory_tracker);
    copy->source_rows.assign(source_rows);
    copy->hashes.assign(hashes);
    copy->buckets.assign(buckets);
    copy->multiplicities.assign(multiplicities);
    copy->key_bytes.assign(key_bytes);
    copy->key_offsets.assign(key_offsets);
    copy->key_sizes.assign(key_sizes);
    copy->key_bytes_source = key_bytes_source;
    copy->fixed_key_size = fixed_key_size;
    copy->constant_key = constant_key;
    return copy;
}

void Aggregator::initializeAdaptiveHeaders(const Block & input_header)
{
    /// The methods whose hashing state reads keys in place from a `ColumnString`, matching the
    /// kernels' `adaptive_key_bytes_in_column`. Nullable and low-cardinality string keys are not
    /// admitted to the adaptive path, so their methods are not listed.
    using Type = AggregatedDataVariants::Type;
    if (method_chosen == Type::key_string || method_chosen == Type::key_packed_string)
        adaptive_key_column_position = input_header.getPositionByName(params.keys.front());

    Block arguments;
    Block staged;
    if (is_simple_count)
        staged.insert({std::make_shared<DataTypeUInt32>(), "multiplicity"});
    else
    {
        for (const auto & positions : aggregates_positions)
            adaptive_argument_positions.insert(adaptive_argument_positions.end(), positions.begin(), positions.end());
        std::sort(adaptive_argument_positions.begin(), adaptive_argument_positions.end());
        adaptive_argument_positions.erase(
            std::unique(adaptive_argument_positions.begin(), adaptive_argument_positions.end()), adaptive_argument_positions.end());
        for (const auto position : adaptive_argument_positions)
        {
            const auto & column = input_header.getByPosition(position);
            arguments.insert(column.cloneEmpty());
            staged.insert({recursiveRemoveLowCardinality(column.type), column.name});
        }
        staged_aggregates_positions = aggregates_positions;
        for (auto & positions : staged_aggregates_positions)
            for (auto & position : positions)
                position = std::lower_bound(adaptive_argument_positions.begin(), adaptive_argument_positions.end(), position)
                    - adaptive_argument_positions.begin();
    }
    if (adaptive_key_column_position)
        arguments.insert(input_header.getByPosition(*adaptive_key_column_position).cloneEmpty());
    adaptive_argument_header = std::make_shared<const Block>(std::move(arguments));
    adaptive_staged_header = std::make_shared<const Block>(std::move(staged));
}

void Aggregator::extractAdaptiveArguments(Chunk & chunk, ColumnPtr key_column) const
{
    chassert((key_column != nullptr) == adaptive_key_column_position.has_value());
    const size_t rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    Columns forwarded;
    forwarded.reserve(adaptive_argument_positions.size() + adaptive_key_column_position.has_value());
    for (const auto position : adaptive_argument_positions)
        forwarded.push_back(std::move(columns[position]));
    if (key_column)
        forwarded.push_back(std::move(key_column));
    chunk.setColumns(std::move(forwarded), rows);
}

ChunkInfo::Ptr StagedKeysInfo::clone() const
{
    auto copy = std::make_shared<StagedKeysInfo>(use_own_memory_tracker);
    copy->keys.routing_hashes.assign(keys.routing_hashes);
    copy->keys.key_bytes.assign(keys.key_bytes);
    copy->keys.key_offsets.assign(keys.key_offsets);
    copy->keys.fixed_key_size = keys.fixed_key_size;
    copy->keys.bucket_offsets = keys.bucket_offsets;
    return copy;
}

Chunk Aggregator::partitionAdaptiveBlock(AdaptiveAggregationSession & shared, StagedChunkConverter & converter, Chunk chunk) const
{
    auto misses = chunk.getChunkInfos().getSafe<AdaptiveAggregationMissesInfo>();
    std::optional<MemoryTrackerSwitcher> memory_tracker_switcher;
    if (misses->use_own_memory_tracker)
        memory_tracker_switcher.emplace(memory_tracker.get());

    Chunk result;
    if (!misses->empty())
    {
        /// The forwarded columns are the arguments followed by the key column when the misses
        /// read their key bytes from it.
        const auto & columns = chunk.getColumns();
        const std::span<const ColumnPtr> arguments(columns.data(), adaptive_argument_positions.size());
        const IColumn * key_column = adaptive_key_column_position ? columns.back().get() : nullptr;
        result = converter.build(arguments, key_column, *misses, is_simple_count);
        const auto info = result.getChunkInfos().get<StagedKeysInfo>();
        const size_t total = misses->size();
        size_t batch_bytes = converter.getRecordedKeyBytes();
        if (is_simple_count)
            batch_bytes += total * sizeof(UInt32);
        else
            for (const auto & column : result.getColumns())
                if (!column->valuesHaveFixedSize())
                    batch_bytes += column->byteSize();
        batch_bytes += total * (sizeof(UInt64) + (info->keys.fixed_key_size ? 0 : sizeof(UInt64)));
        observeAdaptiveStagedRecords(shared, misses->getHashes(), batch_bytes);

        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecords, total);
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecordsMerged, total - result.getNumRows());
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedBytes, info->keys.key_bytes.size());
    }
    else
    {
        /// A block without misses is forwarded only under memory pressure, so that the empty
        /// chunk reaches the coalescer and flushes the candidates it buffered earlier.
        result = Chunk(adaptive_staged_header->getColumns(), 0);
        auto info = std::make_shared<StagedKeysInfo>(misses->use_own_memory_tracker);
        info->keys.key_offsets.push_back(0);
        result.getChunkInfos().add(std::move(info));
    }
    chunk.getChunkInfos().clear();
    misses.reset();
    return result;
}

void Aggregator::publishAdaptiveChunk(AdaptiveAggregationSession & shared, Chunk chunk) const
{
    auto info = chunk.getChunkInfos().getSafe<StagedKeysInfo>();
    std::optional<MemoryTrackerSwitcher> memory_tracker_switcher;
    if (info->use_own_memory_tracker)
        memory_tracker_switcher.emplace(memory_tracker.get());

    chassert(chunk.getNumRows() != 0 && info->keys.size() == chunk.getNumRows());
    auto staged = std::make_shared<StagedChunk>();
    staged->keys = std::move(info->keys);
    if (is_simple_count)
    {
        chassert(chunk.getNumColumns() == 1);
        auto columns = chunk.mutateColumns();
        std::get<StagedChunk::CountPayload>(staged->payload).multiplicities
            = std::move(assert_cast<ColumnUInt32 &>(*columns.front()).getData());
    }
    else
    {
        chassert(chunk.getNumColumns() == adaptive_argument_positions.size());
        staged->payload.emplace<StagedChunk::AggregatePayload>().argument_columns = chunk.detachColumns();
    }
    chunk.getChunkInfos().clear();
    info.reset();
    publishStagedChunk(shared, std::move(staged));
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
