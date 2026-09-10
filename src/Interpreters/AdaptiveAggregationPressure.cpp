#include <algorithm>
#include <limits>
#include <Columns/IColumn.h>
#include <Common/Arena.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <base/arithmeticOverflow.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationPressureDrainedRecords;
    extern const Event AdaptiveAggregationPressureSweeps;
    extern const Event AdaptiveAggregationResidueReleases;
    extern const Event AdaptiveAggregationSharedTableSpills;
    extern const Event AdaptiveAggregationStagedChunkPiecesOverBound;
    extern const Event AdaptiveAggregationStagedClaimsClosedAtBound;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
    extern const int LOGICAL_ERROR;
}

size_t Aggregator::adaptivePressurePartBytes() const
{
    if (!params.max_bytes_before_external_group_by)
        return std::numeric_limits<size_t>::max();

    /// An eighth of the threshold, so that the residue, the batch a sweep claims, the table it
    /// drains that batch into and the writes in flight all fit inside it several times over.
    /// The floor keeps a part worth a file when the threshold itself is small.
    return std::max(params.max_bytes_before_external_group_by / 8, adaptive_pressure_min_part_bytes);
}

/// The hash-table cell of the drain table's variant: what one record occupies in the table's
/// buffer, before the buffer's slack. A `UInt64` key is a 16-byte cell, the fixed `keys128` and
/// `keys256` variants 24 and 40 bytes, and a string key with its saved hash 32. A bound derived
/// from one would be wrong for the others, so it is read from the variant the drains actually build.
/// The drain tables are always the two-level twin of the shared method.
static size_t adaptiveDrainCellBytes(AggregatedDataVariants::Type type)
{
    switch (type)
    {
#define M(NAME) \
        case AggregatedDataVariants::Type::NAME: \
            return sizeof(typename decltype(AggregatedDataVariants::NAME)::element_type::Data::cell_type);

        APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M

        default:
            throw Exception(
                ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant in the adaptive drain path.");
    }
}

/// The bytes one drained record costs in the destination table: its cell in the hash-table
/// buffer, which the grower keeps at most half full (`HashTableGrower::maxFill`), so two cells
/// per record; and its aggregate states in the arena. Variable-width key bytes and the staged
/// payload are charged by the caller from the batch itself (see `estimateStagedBytesWithKeyCopy`).
size_t Aggregator::adaptiveDrainRecordBytes(AggregatedDataVariants::Type type) const
{
    return 2 * adaptiveDrainCellBytes(type) + total_size_of_aggregate_states;
}

size_t Aggregator::adaptivePressurePartRecords(AggregatedDataVariants::Type type) const
{
    const size_t part_bytes = adaptivePressurePartBytes();
    if (part_bytes == std::numeric_limits<size_t>::max())
        return adaptive_pressure_spill_min_keys;

    /// The records whose cells and states alone fill a part. Their variable-width key bytes
    /// are not known where this is used, so for wide string keys this overestimates and the
    /// byte-side claim beside it (`estimateAdaptiveDrainBytes`) and the detach's reading of
    /// the built table's real `allocatedBytes` are what stop the overshoot. Never below one
    /// record per bucket, so that a batch stays worth its bucket-major pass however wide the
    /// keys or the states are.
    const size_t per_record_bytes = adaptiveDrainRecordBytes(type);
    return std::clamp(part_bytes / per_record_bytes, ADAPTIVE_AGGREGATION_NUM_BUCKETS, adaptive_pressure_spill_min_keys);
}

size_t Aggregator::adaptivePressureDetachedBytesBudget() const
{
    const size_t part_bytes = adaptivePressurePartBytes();
    if (part_bytes == std::numeric_limits<size_t>::max())
        return adaptive_pressure_detached_bytes_budget;

    /// Room for a couple of parts in flight, never more than the absolute ceiling: a query that
    /// asked to spill at the threshold cannot afford to hold multiples of it undrained.
    return std::min(adaptive_pressure_detached_bytes_budget, std::max(params.max_bytes_before_external_group_by / 2, 2 * part_bytes));
}

size_t Aggregator::estimateAdaptiveDrainBytes(AggregatedDataVariants::Type type, size_t records, size_t staged_bytes) const
{
    const size_t per_record_bytes = adaptiveDrainRecordBytes(type);
    size_t estimated_bytes = 0;
    if (common::mulOverflow(records, per_record_bytes, estimated_bytes)
        || common::addOverflow(estimated_bytes, staged_bytes, estimated_bytes))
        return std::numeric_limits<size_t>::max();
    return estimated_bytes;
}

/// The bytes the calling thread holds as its own memory tracker counts them, with the untracked
/// tail flushed so that two readings around a piece of work bound what it allocated and kept.
/// Unlike a table's `allocatedBytes`, which sums its arenas and hash-table buffers, this sees
/// the heap that states such as `uniqExact` or `groupBitmap` own outside the arenas.
static Int64 currentThreadTrackedMemory()
{
    if (!CurrentThread::isInitialized())
        return 0;
    auto & thread = CurrentThread::get();
    thread.flushUntrackedMemory();
    chassert(thread.memory_tracker.level == VariableContext::Thread);
    return thread.memory_tracker.get();
}

/// The shared drain table's footprint for the part bound and the detached-bytes budget: the
/// larger of what the table reports and what the drains into it were seen to allocate. Read
/// under `pressure_sweep_mutex`.
static size_t sharedDrainTableBytes(const AdaptiveAggregationSession & shared)
{
    return std::max(shared.early_drain_variants->allocatedBytes(), shared.early_drain_tracked_bytes);
}

/// Swaps an empty table in for the shared drain table and hands the full one back, with its
/// tracked account starting over. Under `pressure_sweep_mutex`.
static AggregatedDataVariantsPtr detachSharedDrainTable(AdaptiveAggregationSession & shared, AggregatedDataVariantsPtr replacement)
{
    auto full = std::move(shared.early_drain_variants);
    shared.early_drain_variants = std::move(replacement);
    shared.early_drain_tracked_bytes = 0;
    return full;
}

/// The memory a published chunk holds, and keeps holding until the drain that claimed it
/// returns: the staged keys and the payload, consisting of run lengths for a count-only chunk
/// or argument columns gathered during conversion. Variable-width arguments can outweigh the
/// drained table's footprint.
/// Variable-width keys are counted twice, because a pressure-time drain copies them into the
/// table's arena while the chunk still holds the staged bytes, so both copies are resident when
/// the drain returns; a fixed-size key lives in the table's cell, which the per-record charge
/// of `Aggregator::adaptiveDrainRecordBytes` already covers.
static size_t estimateStagedBytesWithKeyCopy(const StagedChunk & chunk)
{
    const size_t copied_key_bytes = chunk.keys.fixed_key_size ? 0 : chunk.keys.key_bytes.allocated_bytes();
    return chunk.allocatedBytes() + copied_key_bytes;
}

/// Estimates the staged bytes and copied keys for a record range. Unlike a whole chunk's allocation,
/// a range has no capacity of its own, so this uses logical sizes. Variable-width keys count twice
/// because the pressure drain copies them into its arena. Argument bytes are summed per record;
/// prorating a chunk's bytes would miss skewed payloads concentrated in a few records or buckets.
/// This scan runs only while splitting a chunk, over disjoint bucket ranges.
static size_t estimateStagedRangeBytesWithKeyCopy(const StagedChunk & chunk, size_t begin, size_t end)
{
    const size_t records = end - begin;
    const size_t key_bytes = chunk.keys.keyByteOffsetAt(end) - chunk.keys.keyByteOffsetAt(begin);
    size_t bytes = records * sizeof(UInt64) + key_bytes;
    if (!chunk.keys.fixed_key_size)
        bytes += key_bytes + (records + 1) * sizeof(UInt64);

    if (chunk.countsOnly())
        return bytes + records * sizeof(UInt32);

    for (const auto & column : std::get<StagedChunk::AggregatePayload>(chunk.payload).argument_columns)
        for (size_t i = begin; i < end; ++i)
            bytes += column->byteSizeAt(i);
    return bytes;
}

std::vector<MutableStagedChunkPtr> Aggregator::splitStagedChunkAtPartBound(
    const AdaptiveAggregationSession & shared, const StagedChunk & chunk) const
{
    /// Pressure drains claim whole chunks and can exceed their part estimate by the last chunk taken.
    /// Split each chunk to fit half that estimate, first at bucket boundaries and then within any
    /// bucket that is too large. A single record stays whole even when it exceeds the estimate.
    /// Coalescing limits staged bytes. The drain estimate also charges hash-table cells and the
    /// inline storage of aggregate states.
    const size_t part_bytes = adaptivePressurePartBytes();
    if (part_bytes == std::numeric_limits<size_t>::max())
        return {};
    const size_t chunk_bound = part_bytes / 2;

    /// The whole chunk is charged as the claim would charge it, by its allocation, so a chunk
    /// the claim would take alone as a full batch is the chunk that is cut.
    const auto type = shared.drain_type;
    const size_t records = chunk.keys.size();
    if (estimateAdaptiveDrainBytes(type, records, estimateStagedBytesWithKeyCopy(chunk)) <= chunk_bound)
        return {};

    /// The pieces as record ranges, each filled greedily: a range takes the next bucket, or the
    /// next record of a bucket being cut, while the estimate for the range with it stays within
    /// the bound, and is closed before the one that would take it over.
    std::vector<std::pair<size_t, size_t>> ranges;
    size_t range_begin = 0;
    size_t range_records = 0;
    size_t range_bytes = 0;
    const auto close_range_before = [&](size_t record)
    {
        ranges.emplace_back(range_begin, record);
        range_begin = record;
        range_records = 0;
        range_bytes = 0;
    };
    const auto fits = [&](size_t more_records, size_t more_bytes)
    {
        return estimateAdaptiveDrainBytes(type, range_records + more_records, range_bytes + more_bytes) <= chunk_bound;
    };
    for (size_t b = 0; b < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++b)
    {
        const size_t bucket_records = chunk.keys.recordsForBucket(b);
        if (!bucket_records)
            continue;

        const size_t bucket_begin = chunk.keys.bucket_offsets[b];
        const size_t bucket_end = chunk.keys.bucket_offsets[b + 1];
        const size_t bucket_bytes = estimateStagedRangeBytesWithKeyCopy(chunk, bucket_begin, bucket_end);
        if (fits(bucket_records, bucket_bytes))
        {
            range_records += bucket_records;
            range_bytes += bucket_bytes;
            continue;
        }
        if (range_records)
            close_range_before(bucket_begin);
        if (fits(bucket_records, bucket_bytes))
        {
            range_records += bucket_records;
            range_bytes += bucket_bytes;
            continue;
        }

        /// The bucket is over the bound on its own: cut inside it, record by record. A record's
        /// bytes are its slice of the range measure, which for variable-width keys counts the
        /// offset of a one-record range twice, conservatively adding a few bytes per record.
        for (size_t i = bucket_begin; i < bucket_end; ++i)
        {
            const size_t record_bytes = estimateStagedRangeBytesWithKeyCopy(chunk, i, i + 1);
            if (range_records && !fits(1, record_bytes))
                close_range_before(i);
            range_records += 1;
            range_bytes += record_bytes;
        }
    }
    ranges.emplace_back(range_begin, records);

    if (ranges.size() == 1)
        return {};

    std::vector<MutableStagedChunkPtr> pieces;
    pieces.reserve(ranges.size());
    for (const auto & [begin, end] : ranges)
    {
        pieces.push_back(chunk.cut(begin, end - begin));

        /// The piece is measured again as a whole, the way its range was measured bucket by
        /// bucket and record by record, so a piece that comes out over the bound is counted:
        /// with the range sizing exact, that can only be a single record that is over the bound
        /// on its own.
        const auto & piece = *pieces.back();
        const size_t piece_records = piece.keys.size();
        if (estimateAdaptiveDrainBytes(type, piece_records, estimateStagedRangeBytesWithKeyCopy(piece, 0, piece_records)) > chunk_bound)
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedChunkPiecesOverBound);
    }
    return pieces;
}

/// Adds the bucket arenas when a drain first needs them, preserving any arenas already in use.
static void ensureDrainArenas(AggregatedDataVariants & table)
{
    while (table.aggregates_pools.size() < ADAPTIVE_AGGREGATION_NUM_BUCKETS)
        table.aggregates_pools.push_back(std::make_shared<Arena>());
}

size_t Aggregator::drainBatchIntoSharedTable(
    AdaptiveAggregationSession & shared, const std::vector<StagedChunkPtr> & batch,
    PaddedPODArray<AggregateDataPtr> & places_scratch) const
{
    /// Sample before releasing the batch so its deallocation cannot hide growth in aggregate states,
    /// including heap storage outside the table's arenas.
    const Int64 tracked_before_drain = currentThreadTrackedMemory();
    const size_t drained_records
        = drainStagedBatch(*shared.early_drain_variants, batch, shared.cancelled, places_scratch);
    shared.early_drain_tracked_bytes
        += static_cast<size_t>(std::max<Int64>(currentThreadTrackedMemory() - tracked_before_drain, 0));
    return drained_records;
}

AggregatedDataVariantsPtr Aggregator::createAdaptiveDrainTable(AggregatedDataVariants::Type type) const
{
    auto table = std::make_shared<AggregatedDataVariants>();
    table->aggregator = this;
    table->keys_size = params.keys_size;
    table->key_sizes = key_sizes;
    table->init(type);
    ensureDrainArenas(*table);
    return table;
}

void Aggregator::spillDetachedAdaptiveTable(AdaptiveAggregationSession & shared, AggregatedDataVariants & table) const
{
    if (shared.cancelled.load(std::memory_order_relaxed))
        return;

    LOG_TRACE(log, "Adaptive aggregation: writing a detached drain table ({} keys) to disk", table.size());
    consumeToTemporaryFile(table);
}

Aggregator::StagedChunkClaim Aggregator::claimStagedChunksToBound(
    const std::vector<StagedChunkPtr> & chunks,
    size_t begin,
    AggregatedDataVariants::Type type,
    size_t records_target,
    size_t bytes_target) const
{
    StagedChunkClaim claim;
    claim.end = begin;
    for (; claim.end < chunks.size(); ++claim.end)
    {
        const StagedChunk & chunk = *chunks[claim.end];
        const size_t records = claim.records + chunk.keys.size();
        const size_t staged_bytes = claim.staged_bytes + estimateStagedBytesWithKeyCopy(chunk);
        const bool reaches_target
            = records >= records_target || estimateAdaptiveDrainBytes(type, records, staged_bytes) >= bytes_target;

        /// Close before adding a chunk that reaches a target, so two individually large chunks
        /// cannot jointly exceed the drain's part bound. The first chunk is always accepted because
        /// a drain must process at least one whole chunk, even when it alone exceeds the target.
        if (reaches_target && claim.end > begin)
        {
            claim.full = true;
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedClaimsClosedAtBound);
            break;
        }

        claim.records = records;
        claim.staged_bytes = staged_bytes;
        if (reaches_target)
        {
            claim.full = true;
            ++claim.end;
            break;
        }
    }
    return claim;
}

void Aggregator::drainStagedChunksAtFinish(AdaptiveAggregationSession & shared) const
{
    std::unique_lock sweep_lock(shared.pressure_sweep_mutex);

    /// A leftover record with nothing enqueued means the accounting lost track of a chunk; a
    /// leftover after the loop means the drain stopped early. Either would silently drop rows
    /// from the result, so both fail loudly.
    const auto check_nothing_left = [&]
    {
        if (!shared.cancelled.load(std::memory_order_relaxed) && shared.backlog.undrainedRecords() != 0)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Adaptive aggregation: the finish drain left {} staged records behind.",
                shared.backlog.undrainedRecords());
    };

    auto chunks = shared.backlog.takeAllForPressureDrain();
    if (chunks.empty())
    {
        check_nothing_left();
        return;
    }

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPressureSweeps);
    ensureDrainArenas(*shared.early_drain_variants);

    PaddedPODArray<AggregateDataPtr> places_scratch;

    const size_t part_bytes = adaptivePressurePartBytes();
    const AggregatedDataVariants::Type drain_type = shared.early_drain_variants->type;
    const size_t part_records = adaptivePressurePartRecords(drain_type);

    size_t drained_records = 0;
    size_t begin = 0;
    /// A cancelled query stops at the next batch; the leftover chunks drop with this vector.
    /// The in-flight batch is dropped too, not requeued: bucket-major progress means parts of
    /// every one of its chunks may already be in the table.
    while (begin < chunks.size() && !shared.cancelled.load(std::memory_order_relaxed))
    {
        /// Detach and write before absorbing another batch, so only one detached table exists
        /// at a time. The current memory reading is deliberately not consulted: the query is
        /// external already, and skipping the spill during a dip would only let the remainder
        /// grow into one arbitrarily large table.
        if (shared.early_drain_variants->size() >= adaptive_pressure_spill_min_keys
            || sharedDrainTableBytes(shared) >= part_bytes)
        {
            auto full = detachSharedDrainTable(shared, createAdaptiveDrainTable(shared.early_drain_variants->type));
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSharedTableSpills);
            spillDetachedAdaptiveTable(shared, *full);
        }

        /// The batch is capped at the table's remaining capacity to the part bound, with a
        /// quarter of it as the minimum so a batch stays worth its bucket-major pass; the claim
        /// closes before the chunk that would take it over the cap, so a part overshoots by no
        /// more than a single chunk that is over the cap alone. The cap is read in records and
        /// in bytes both, the same way the pressure sweep claims: a record count derived from
        /// the fixed state width says nothing about a backlog of wide keys or wide staged
        /// arguments, which would otherwise rebuild here the over-budget working set the sweeps
        /// exist to avoid.
        const size_t table_records = shared.early_drain_variants->size();
        const size_t table_bytes = sharedDrainTableBytes(shared);
        const size_t batch_target = std::max(part_records - std::min(part_records, table_records), part_records / 4 + 1);
        const size_t batch_bytes_target = std::max(part_bytes - std::min(part_bytes, table_bytes), part_bytes / 4 + 1);

        const size_t end = claimStagedChunksToBound(chunks, begin, drain_type, batch_target, batch_bytes_target).end;

        const std::vector<StagedChunkPtr> batch(
            std::make_move_iterator(chunks.begin() + begin), std::make_move_iterator(chunks.begin() + end));
        drained_records += drainBatchIntoSharedTable(shared, batch, places_scratch);
        begin = end;
    }

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPressureDrainedRecords, drained_records);
    shared.backlog.recordDrained(drained_records);
    LOG_TRACE(log, "Adaptive aggregation: finish drain converted {} staged records", drained_records);

    check_nothing_left();
}

size_t Aggregator::drainStagedChunksUnderMemoryPressure(AdaptiveAggregationSession & shared) const
{
    PaddedPODArray<AggregateDataPtr> places_scratch;

    /// A sweep sheds until the query is back under the threshold or the backlog is down to a
    /// tail, one part at a time. One claim per sweep would not do: a sweep runs once per consumed
    /// block, a block can publish more than a part of chunks (a wide block is cut into pieces of
    /// half a part each, see `splitStagedChunkAtPartBound`), and the difference would stay in the
    /// backlog and grow with every block.
    size_t drained_records = 0;
    while (true)
    {
        /// The claim that ends the sweep drains the tail, so its records count too.
        size_t batch_records = 0;
        const bool claim_again = drainStagedChunksBatchUnderMemoryPressure(shared, places_scratch, batch_records);
        drained_records += batch_records;
        if (!claim_again)
            break;
    }
    return drained_records;
}

bool Aggregator::drainStagedChunksBatchUnderMemoryPressure(
    AdaptiveAggregationSession & shared, PaddedPODArray<AggregateDataPtr> & places_scratch, size_t & drained_records_out) const
{
    drained_records_out = 0;
    const size_t part_bytes = adaptivePressurePartBytes();

    /// The coordinator lock is held only to claim work: a batch of chunks carrying about one
    /// part's worth of records. Full batches are drained into a producer-local table and
    /// written entirely outside the lock, so the transformation and the writes of successive
    /// batches run in parallel across the producers that hit the trigger; only a tail too
    /// small for a part is drained into the shared table under the lock, where its residue
    /// keeps accumulating toward a part instead of fragmenting per producer.
    std::vector<StagedChunkPtr> batch;
    size_t batch_records = 0;
    size_t estimated_bytes = 0;
    AggregatedDataVariants::Type routing_type = AggregatedDataVariants::Type::EMPTY;
    {
        std::unique_lock sweep_lock(shared.pressure_sweep_mutex);
        if (getCurrentQueryMemoryUsage() < static_cast<Int64>(params.max_bytes_before_external_group_by))
            return false;

        auto chunks = shared.backlog.takeAllForPressureDrain();
        if (chunks.empty())
            return false;

        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPressureSweeps);

        /// Each claim targets a record count and an estimated drain footprint. The estimate includes
        /// gathered argument columns and keys because the batch retains them throughout `drainStagedBatch`.
        /// A claim that reaches either bound, or closes before a chunk that would reach one, drains
        /// into its own table. A claim that exhausts the backlog below both bounds accumulates in the
        /// shared table.
        routing_type = shared.early_drain_variants->type;
        const auto claim = claimStagedChunksToBound(chunks, 0, routing_type, adaptive_pressure_spill_min_keys, part_bytes);
        const bool batch_is_full = claim.full;
        const size_t batch_staged_bytes = claim.staged_bytes;
        const size_t split = claim.end;
        batch_records = claim.records;
        batch.assign(std::make_move_iterator(chunks.begin()), std::make_move_iterator(chunks.begin() + split));
        for (size_t i = split; i < chunks.size(); ++i)
            shared.backlog.requeue(chunks[i]);

        if (!batch_is_full)
        {
            /// The tail regime: too little for a part of reasonable size.
            ensureDrainArenas(*shared.early_drain_variants);
            const size_t drained_records = drainBatchIntoSharedTable(shared, batch, places_scratch);
            batch.clear();

            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPressureDrainedRecords, drained_records);
            shared.backlog.recordDrained(drained_records);
            drained_records_out = drained_records;
            LOG_TRACE(log, "Adaptive aggregation: pressure sweep drained {} staged records early", drained_records);

            /// Tail drains can push the shared residue past the floor over time; detach it
            /// under the lock and write it outside, like a producer-local table. The
            /// reservation waits if it must: skipping here would let later tails grow the
            /// shared table without bound, and waiting while holding the coordinator lock is
            /// safe because writers release their reservations through `detached_spill_mutex`
            /// alone. Only cancellation declines.
            AggregatedDataVariantsPtr detached_shared;
            AdaptiveAggregationSession::SpillReservation reservation;
            const size_t residue_bytes = sharedDrainTableBytes(shared);
            if ((shared.early_drain_variants->size() >= adaptive_pressure_spill_min_keys || residue_bytes >= part_bytes)
                && reservation.reserveOrWait(shared, residue_bytes, adaptivePressureDetachedBytesBudget()))
            {
                detached_shared = detachSharedDrainTable(shared, createAdaptiveDrainTable(shared.early_drain_variants->type));
                ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSharedTableSpills);
            }

            sweep_lock.unlock();
            if (detached_shared)
                spillDetachedAdaptiveTable(shared, *detached_shared);
            return false;
        }

        /// Saturating rather than wrapping, and a request larger than the whole budget is
        /// granted when it is alone, so an absurd estimate cannot deadlock the valve.
        estimated_bytes = estimateAdaptiveDrainBytes(routing_type, batch_records, batch_staged_bytes);
    }

    /// The budget is claimed with the coordinator lock released, so a producer that must wait
    /// for a writer does not block the other producers' claims; staging is paused either way,
    /// which is the backpressure that keeps the backlog bounded under slow storage. The wait
    /// ends only with a grant or with cancellation.
    AdaptiveAggregationSession::SpillReservation reservation;
    if (!reservation.reserveOrWait(shared, estimated_bytes, adaptivePressureDetachedBytesBudget()))
    {
        for (auto & chunk : batch)
            shared.backlog.requeue(chunk);
        return false;
    }

    auto local = createAdaptiveDrainTable(routing_type);

    /// The drain runs on this thread alone, so the growth of its own tracked memory across the
    /// call is what the built table costs in full: the arenas and buckets its `allocatedBytes`
    /// reports, and the heap that states owning memory outside the arenas allocated for their
    /// groups, which neither the estimate nor `allocatedBytes` can see. Read before the staged
    /// batch is released, so its bytes do not come off the reading.
    const Int64 tracked_before_drain = currentThreadTrackedMemory();
    const size_t drained_records = drainStagedBatch(*local, batch, shared.cancelled, places_scratch);
    const Int64 drain_growth = currentThreadTrackedMemory() - tracked_before_drain;
    /// Release the staged memory before the write, not after; the batch is dropped rather
    /// than requeued even when cancellation stopped the drain early, because bucket-major
    /// progress means parts of every chunk may already be in the table.
    batch.clear();

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPressureDrainedRecords, drained_records);
    shared.backlog.recordDrained(drained_records);
    drained_records_out = drained_records;
    LOG_TRACE(log, "Adaptive aggregation: pressure sweep drained {} staged records into a producer-local table", drained_records);

    /// Raise the reservation to cover both the table's reported size and the observed allocation
    /// growth. Keep at least the original estimate so the serialization scratch still to come is
    /// not reserved by another writer.
    const size_t drain_growth_bytes = static_cast<size_t>(std::max<Int64>(drain_growth, 0));
    reservation.resize(std::max({estimated_bytes, local->allocatedBytes(), drain_growth_bytes}));

    if (drained_records)
        spillDetachedAdaptiveTable(shared, *local);

    /// A full batch was shed; whether the backlog holds another is for the next claim to see.
    return !shared.cancelled.load(std::memory_order_relaxed);
}

std::optional<Int64> Aggregator::releaseAdaptiveDrainResidue(AdaptiveAggregationSession & shared) const
{
    /// The shared table is created by the first freeze, so in a session where no producer ever
    /// froze there is none: a learning thread can stand down under memory pressure alone.
    if (!shared.initialized.load(std::memory_order_acquire))
        return {};

    std::unique_lock sweep_lock(shared.pressure_sweep_mutex);

    /// Read under the coordinator lock: the sweeps replace this pointer while holding it. They
    /// detach only at the part bound, so a residue below it is never written by them and stays
    /// resident until the merge.
    while (shared.early_drain_variants->hasData())
    {
        /// Declared before the table so that reverse-order destruction frees the table first:
        /// the budget is handed on only once the bytes it stands for are really gone.
        AdaptiveAggregationSession::SpillReservation reservation;
        AggregatedDataVariantsPtr detached;

        if (!reservation.reserveOrWait(shared, sharedDrainTableBytes(shared), adaptivePressureDetachedBytesBudget()))
            return {};

        detached = detachSharedDrainTable(shared, createAdaptiveDrainTable(shared.early_drain_variants->type));

        /// Writing under the coordinator lock would stall every frozen producer's sweep for the
        /// length of a disk write; only reservations may wait under it.
        sweep_lock.unlock();
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationResidueReleases);
        spillDetachedAdaptiveTable(shared, *detached);
        detached.reset();
        reservation.release();
        sweep_lock.lock();
    }

    /// The whole budget is granted only when no detached table or reserved writer is in flight, and
    /// holding it keeps a new detach from starting, while the coordinator lock keeps a sweep from
    /// refilling the table, so memory already committed to a write cannot enter the reading.
    AdaptiveAggregationSession::SpillReservation quiesce;
    const size_t detached_budget = adaptivePressureDetachedBytesBudget();
    if (!quiesce.reserveOrWait(shared, detached_budget, detached_budget))
        return {};

    return getCurrentQueryMemoryUsage();
}

}
