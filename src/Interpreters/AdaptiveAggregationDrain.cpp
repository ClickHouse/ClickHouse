/// The drain side of the adaptive aggregation: the merge-time and pressure-time consumption
/// of staged chunks (see `AdaptiveAggregationDrain.h`). The drainer's method-specialized
/// templates are defined here, instantiated by its own non-templated entry points.

#include <Common/Arena.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <base/unaligned.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationDrainedRecords;
    extern const Event AdaptiveAggregationBucketsRetired;
    extern const Event AdaptiveAggregationPressureSweeps;
    extern const Event AdaptiveAggregationPressureDrainedRecords;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
    extern const int LOGICAL_ERROR;
}

}

namespace
{
    /// Applies `f` to the two-level method the variant currently holds. The adaptive session
    /// only ever materializes two-level variants, so any other type is a logical error.
    template <typename F>
    void visitTwoLevelVariant(DB::AggregatedDataVariants & variants, F && f)
    {
#define M(NAME) \
    else if (variants.type == DB::AggregatedDataVariants::Type::NAME) \
        f(*variants.NAME);

        if (false) {} /// NOLINT
        APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
        else
            throw DB::Exception(
                DB::ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant in the adaptive drain path.");
    }

    /// Emplace one staged key into the table. String-like keys were staged as raw characters
    /// and are rebuilt here. `key_storage` selects the ownership: at merge time the delayed
    /// blocks are retained on the shared state until after the merged buckets are converted, so
    /// string-like keys are emplaced pointing into the staged bytes directly, with no copy; a
    /// pressure-time drain instead persists them into the arena, because freeing the blocks is
    /// its purpose. Fixed-size keys were staged as values either way.
    /// `table` is the bucket's own submap: the records were grouped by the same hash dispatch
    /// at staging time, so emplacing into it directly skips the per-record two-level routing.
    /// Prefetch the table slot of the record `prefetch_look_ahead` positions ahead of `j`, if
    /// any: hash-organized tables prefetch by the saved routing hash, string tables locate the
    /// slot from the key bytes and the hash. The two drain loops share this so the dispatch
    /// cannot drift between them.
    /// Position and width of record j's staged key bytes. A fixed-size-key chunk carries no
    /// offsets array, and in the drains the width is the compile-time key width, so the
    /// position is a plain multiplication.
    template <typename Key>
    ALWAYS_INLINE std::pair<const char *, size_t> stagedKeyAt(const DB::StagedChunk::StagedKeys & keys, size_t j)
    {
        if constexpr (DB::adaptive_key_stages_bytes<Key>)
            return {keys.key_bytes.data() + keys.key_offsets[j], keys.key_offsets[j + 1] - keys.key_offsets[j]};
        else
            return {keys.key_bytes.data() + j * sizeof(Key), sizeof(Key)};
    }

    template <typename Key, typename Impl>
    void ALWAYS_INLINE prefetchStagedKey(Impl & impl, const DB::StagedChunk::StagedKeys & keys, size_t j, size_t slice_end)
    {
        const size_t la = j + DB::adaptive_drain_prefetch_look_ahead;
        if (la >= slice_end)
            return;
        if constexpr (requires { impl.prefetchByHash(keys.routing_hashes[j]); })
            impl.prefetchByHash(keys.routing_hashes[la]);
        else if constexpr (std::is_same_v<Key, std::string_view>)
            impl.prefetch(keys.keyBytesAt(la), keys.routing_hashes[la]);
    }

    template <typename Key, DB::AdaptiveKeyStorage key_storage, typename Table>
    void ALWAYS_INLINE emplaceStagedKey(
        Table & table,
        const char * key_pos,
        size_t key_size,
        size_t routing_hash,
        DB::Arena & arena,
        typename Table::LookupResult & it,
        bool & inserted)
    {
        if constexpr (std::is_same_v<Key, std::string_view>)
        {
            if constexpr (key_storage == DB::AdaptiveKeyStorage::BorrowFromChunk)
                table.emplace(std::string_view(key_pos, key_size), it, inserted, routing_hash);
            else
                table.emplace(DB::ArenaKeyHolder{std::string_view(key_pos, key_size), arena}, it, inserted, routing_hash);
        }
        else if constexpr (std::is_same_v<Key, PackedStringRef>)
        {
            /// The staged routing hash IS the packed key's cached content hash
            /// (`DefaultHash<PackedStringRef>` returns it), so the rebuild reuses it instead of
            /// re-hashing the key bytes; `build` consults the functor only for lengths that
            /// store a hash, which is exactly the range the staged hash was derived from.
            const auto key = PackedStringRef::build(
                key_pos, key_size, [routing_hash](const char *, size_t) { return static_cast<UInt32>(routing_hash); });
            if constexpr (key_storage == DB::AdaptiveKeyStorage::BorrowFromChunk)
                table.emplace(key, it, inserted, routing_hash);
            else
                table.emplace(DB::ArenaPackedStringHolder{key, arena}, it, inserted, routing_hash);
        }
        else
        {
            table.emplace(unalignedLoad<Key>(key_pos), it, inserted, routing_hash);
        }
    }
}

namespace DB
{

void StagedChunkDrainer::retireMergedBucket(AggregatedDataVariants & dest, size_t bucket) const
{
    dest.adaptive_merge_bucket_arenas[bucket].reset();
    session.backlog.releaseMergedBucket(bucket);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationBucketsRetired);
}

void StagedChunkDrainer::drainBucketForMerge(
    AggregatedDataVariants & dest,
    Arena * arena,
    size_t bucket_index,
    const StagedSliceApplier & applier,
    std::atomic<bool> & is_cancelled) const
{
    if (is_cancelled.load(std::memory_order_relaxed))
        return;

    const auto & backlog = session.backlog.forMergeBucket(bucket_index);
    if (backlog.empty())
        return;

    PaddedPODArray<AggregateDataPtr> places_scratch;

    size_t records_available = 0;
    for (const auto & block : backlog)
        records_available += block->keys.recordsForBucket(bucket_index);

    size_t drained = 0;
    visitTwoLevelVariant(
        dest,
        [&](auto & method)
        {
            drained = drainBucketBacklog<AdaptiveKeyStorage::BorrowFromChunk>(
                method, arena, backlog, bucket_index, records_available, places_scratch, applier, is_cancelled);
        });

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationDrainedRecords, drained);
    session.backlog.recordDrained(drained);
}

template <AdaptiveKeyStorage key_storage, typename Method>
size_t NO_INLINE StagedChunkDrainer::drainBucketBacklog(
    Method & method,
    Arena * arena,
    const std::vector<StagedChunkPtr> & backlog,
    size_t bucket_index,
    size_t total_records,
    PaddedPODArray<AggregateDataPtr> & places,
    const StagedSliceApplier & applier,
    std::atomic<bool> & is_cancelled) const
{
    auto & impl = method.data.impls[bucket_index];

    const size_t reserve_sample_records = total_records / adaptive_reserve_sample_inverse;
    const size_t size_before = impl.size();
    bool reserved = false;
    size_t processed = 0;
    size_t sampled_string_view_keys = 0;

    auto update_reserve = [&](size_t rows)
    {
        processed += rows;
        if (!reserved && processed >= reserve_sample_records && processed < total_records)
        {
            reserved = true;
            const double insert_rate = static_cast<double>(impl.size() - size_before) / static_cast<double>(processed);
            const auto expected
                = static_cast<size_t>(static_cast<double>(total_records - processed) * insert_rate * adaptive_reserve_headroom);

            /// A string table cannot pre-size as a whole: its short keys spread over the
            /// length-classed submaps, whose shares the sampling does not see. Only the
            /// raw-string submap is identifiable per record, so the sampling counts the records
            /// destined for it and only that submap is reserved; the short-key submaps grow by
            /// their ordinary rehashing.
            if constexpr (requires { impl.reserveAdditionalStringViewKeys(size_t{}); })
            {
                const double string_view_fraction = static_cast<double>(sampled_string_view_keys) / static_cast<double>(processed);
                impl.reserveAdditionalStringViewKeys(static_cast<size_t>(static_cast<double>(expected) * string_view_fraction));
            }
            else
                impl.reserve(impl.size() + expected);
        }
    };

    size_t drained = 0;
    for (const auto & block_ptr : backlog)
    {
        if (is_cancelled.load(std::memory_order_relaxed))
            return drained;

        const auto & block = *block_ptr;
        const auto & keys = block.keys;
        /// The chunk's key representation must match the method this drain resolves keys for:
        /// `stagedKeyAt` derives fixed-key positions from the compile-time key width.
        chassert(keys.fixed_key_size == (DB::adaptive_key_stages_bytes<typename Method::Key> ? 0 : sizeof(typename Method::Key)));
        const size_t slice_begin = keys.bucket_offsets[bucket_index];
        const size_t slice_end = keys.bucket_offsets[bucket_index + 1];

        if constexpr (requires { impl.reserveAdditionalStringViewKeys(size_t{}); })
        {
            /// Sampled only while a reserve can still fire: `update_reserve` requires unseen
            /// records to remain, so the final block of a backlog would count keys nothing
            /// ever reads.
            if (!reserved && processed + (slice_end - slice_begin) < total_records)
            {
                for (size_t j = slice_begin; j < slice_end; ++j)
                    if (impl.usesStringViewSubmap(keys.keyBytesAt(j)))
                        ++sampled_string_view_keys;
            }
        }

        if (const auto * counts = std::get_if<StagedChunk::CountPayload>(&block.payload))
        {
            const auto & multiplicities = counts->multiplicities;
            for (size_t j = slice_begin; j < slice_end; ++j)
            {
                prefetchStagedKey<typename Method::Key>(impl, keys, j, slice_end);

                const auto [key_data, key_size] = stagedKeyAt<typename Method::Key>(keys, j);
                typename Method::Data::LookupResult it;
                bool inserted = false;
                emplaceStagedKey<typename Method::Key, key_storage>(
                    impl, key_data, key_size, keys.routing_hashes[j], *arena, it, inserted);

                if (inserted)
                    getInlineCountState(it->getMapped()) = multiplicities[j];
                else
                    getInlineCountState(it->getMapped()) += multiplicities[j];
            }
        }
        else
        {
            drainBucketSlice<key_storage>(method, arena, block, slice_begin, slice_end, places, bucket_index, applier);
        }

        update_reserve(slice_end - slice_begin);
        drained += slice_end - slice_begin;
    }

    return drained;
}

template <AdaptiveKeyStorage key_storage, typename Method>
void NO_INLINE StagedChunkDrainer::drainBucketSlice(
    Method & method,
    Arena * bucket_arena,
    const StagedChunk & block,
    size_t slice_begin,
    size_t slice_end,
    PaddedPODArray<AggregateDataPtr> & places,
    size_t bucket_index,
    const StagedSliceApplier & applier) const
{
    auto & impl = method.data.impls[bucket_index];
    const auto & keys = block.keys;

    const auto & prep = *std::get<StagedChunk::AggregatePayload>(block.payload).prepared;

    /// The consume path's compiled aggregation applies here under the same gate: unlike the
    /// frozen consume loop, whose misses are null places the compiled row loop cannot skip,
    /// every place in a drain slice is non-null, and the staged argument columns are always
    /// dense. The sparse check only mirrors the consume-path gate - a prepared staged chunk
    /// cannot carry sparse arguments.
    const bool use_compiled_functions = applier.useCompiledFunctions(prep.instructions.data());

    /// `places` is indexed by absolute record index: the compacted argument columns hold record
    /// j's values at row j, so the batch calls below consume the [slice_begin, slice_end) range
    /// of the columns and of `places` directly.
    places.resize(slice_end);

    for (size_t j = slice_begin; j < slice_end; ++j)
    {
        prefetchStagedKey<typename Method::Key>(impl, keys, j, slice_end);
        const auto [key_data, key_size] = stagedKeyAt<typename Method::Key>(keys, j);
        typename Method::Data::LookupResult it;
        bool inserted = false;
        emplaceStagedKey<typename Method::Key, key_storage>(
            impl, key_data, key_size, keys.routing_hashes[j], *bucket_arena, it, inserted);

        AggregateDataPtr aggregate_data = nullptr;
        if (inserted)
        {
            it->getMapped() = nullptr;
            aggregate_data = applier.createStates(*bucket_arena, use_compiled_functions);
            it->getMapped() = aggregate_data;
        }
        else
            aggregate_data = it->getMapped();

        places[j] = aggregate_data;
    }

    if (!applier.aggregatesSize())
        return;

    /// Apply the aggregate functions to the delayed rows only: the slice is a contiguous row
    /// range of the compacted argument columns and of `places`, so the standard executor
    /// applies to it directly - one compiled row loop for the compiled functions, a batch pass
    /// per remaining function.
    applier.applyInstructions(bucket_arena, slice_begin, slice_end, prep.instructions.data(), places.data(), use_compiled_functions);
}

AggregatedDataVariantsPtr Aggregator::createAdaptiveDrainTable(AggregatedDataVariants::Type type) const
{
    auto table = std::make_shared<AggregatedDataVariants>();
    table->aggregator = this;
    table->keys_size = params.keys_size;
    table->key_sizes = key_sizes;
    table->init(type);
    /// Bucket b's drained states live in pool b, mirroring the merge-time layout.
    while (table->aggregates_pools.size() < ADAPTIVE_AGGREGATION_NUM_BUCKETS)
        table->aggregates_pools.push_back(std::make_shared<Arena>());
    return table;
}

size_t StagedChunkDrainer::drainBatch(
    AggregatedDataVariants & table,
    const std::vector<StagedChunkPtr> & chunks,
    std::atomic<bool> & is_cancelled,
    PaddedPODArray<AggregateDataPtr> & places_scratch,
    const StagedSliceApplier & applier) const
{
    size_t drained = 0;
    visitTwoLevelVariant(
        table,
        [&](auto & method)
        {
            for (size_t b = 0; b < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++b)
            {
                if (is_cancelled.load(std::memory_order_relaxed))
                    return;

                size_t records = 0;
                for (const auto & chunk : chunks)
                    records += chunk->keys.recordsForBucket(b);
                if (!records)
                    continue;

                drained += drainBucketBacklog<AdaptiveKeyStorage::CopyToArena>(
                    method, table.aggregates_pools.at(b).get(), chunks, b, records, places_scratch, applier, is_cancelled);
            }
        });
    return drained;
}

void Aggregator::spillDetachedAdaptiveTable(AdaptiveAggregationSession & shared, AggregatedDataVariants & table) const
{
    if (shared.cancelled.load(std::memory_order_relaxed))
        return;
    LOG_TRACE(log, "Adaptive aggregation: writing a detached drain table ({} keys) to disk", table.size());
    consumeToTemporaryFile(table);
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
    while (shared.early_drain_variants->aggregates_pools.size() < ADAPTIVE_AGGREGATION_NUM_BUCKETS)
        shared.early_drain_variants->aggregates_pools.push_back(std::make_shared<Arena>());

    PaddedPODArray<AggregateDataPtr> places_scratch;

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
        if (shared.early_drain_variants->size() >= adaptive_pressure_spill_min_keys)
        {
            auto full = std::move(shared.early_drain_variants);
            shared.early_drain_variants = createAdaptiveDrainTable(full->type);
            spillDetachedAdaptiveTable(shared, *full);
        }

        /// The batch is capped at the table's remaining capacity to the floor, with a
        /// quarter-floor minimum so a batch stays worth its bucket-major pass; a part
        /// therefore overshoots the floor by at most a quarter instead of a whole batch.
        const size_t batch_target = std::max(
            adaptive_pressure_spill_min_keys - shared.early_drain_variants->size(), adaptive_pressure_spill_min_keys / 4);

        size_t batch_records = 0;
        size_t end = begin;
        for (; end < chunks.size() && batch_records < batch_target; ++end)
            batch_records += chunks[end]->keys.size();

        const std::vector<StagedChunkPtr> batch(
            std::make_move_iterator(chunks.begin() + begin), std::make_move_iterator(chunks.begin() + end));
        drained_records += StagedChunkDrainer(shared).drainBatch(*shared.early_drain_variants, batch, shared.cancelled, places_scratch, StagedSliceApplier(*this));
        begin = end;
    }

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPressureDrainedRecords, drained_records);
    shared.backlog.recordDrained(drained_records);
    LOG_TRACE(log, "Adaptive aggregation: finish drain converted {} staged records", drained_records);

    check_nothing_left();
}

void Aggregator::drainStagedChunksUnderMemoryPressure(AdaptiveAggregationSession & shared) const
{
    PaddedPODArray<AggregateDataPtr> places_scratch;

    /// The coordinator lock is held only to claim work: a batch of chunks carrying about one
    /// spill floor of records. Floor-sized batches are drained into a producer-local table
    /// and written entirely outside the lock, so the transformation and the writes of
    /// successive batches run in parallel across the producers that hit the trigger; only a
    /// sub-floor tail is drained into the shared table under the lock, where its residue
    /// keeps accumulating toward the floor instead of fragmenting per producer.
    std::vector<StagedChunkPtr> batch;
    size_t batch_records = 0;
    size_t estimated_bytes = 0;
    AggregatedDataVariants::Type routing_type = AggregatedDataVariants::Type::EMPTY;
    {
        std::unique_lock sweep_lock(shared.pressure_sweep_mutex);
        if (getCurrentQueryMemoryUsage() < static_cast<Int64>(params.max_bytes_before_external_group_by))
            return;

        auto chunks = shared.backlog.takeAllForPressureDrain();
        if (chunks.empty())
            return;

        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPressureSweeps);

        size_t batch_key_bytes = 0;
        size_t split = 0;
        for (; split < chunks.size() && batch_records < adaptive_pressure_spill_min_keys; ++split)
        {
            batch_records += chunks[split]->keys.size();
            batch_key_bytes += chunks[split]->keys.key_bytes.size();
        }
        batch.assign(std::make_move_iterator(chunks.begin()), std::make_move_iterator(chunks.begin() + split));
        for (size_t i = split; i < chunks.size(); ++i)
            shared.backlog.requeue(chunks[i]);

        if (batch_records < adaptive_pressure_spill_min_keys)
        {
            /// The tail regime: too little for a part of reasonable size.
            while (shared.early_drain_variants->aggregates_pools.size() < ADAPTIVE_AGGREGATION_NUM_BUCKETS)
                shared.early_drain_variants->aggregates_pools.push_back(std::make_shared<Arena>());

            const size_t drained_records
                = StagedChunkDrainer(shared).drainBatch(*shared.early_drain_variants, batch, shared.cancelled, places_scratch, StagedSliceApplier(*this));
            batch.clear();

            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPressureDrainedRecords, drained_records);
            shared.backlog.recordDrained(drained_records);
            LOG_TRACE(log, "Adaptive aggregation: pressure sweep drained {} staged records early", drained_records);

            /// Tail drains can push the shared residue past the floor over time; detach it
            /// under the lock and write it outside, like a producer-local table. The
            /// reservation waits if it must: skipping here would let later tails grow the
            /// shared table without bound, and waiting while holding the coordinator lock is
            /// safe because writers release their reservations through `detached_spill_mutex`
            /// alone. Only cancellation declines.
            AggregatedDataVariantsPtr detached_shared;
            AdaptiveAggregationSession::SpillReservation reservation;
            if (shared.early_drain_variants->size() >= adaptive_pressure_spill_min_keys
                && reservation.reserveOrWait(shared, shared.early_drain_variants->allocatedBytes()))
            {
                detached_shared = std::move(shared.early_drain_variants);
                shared.early_drain_variants = createAdaptiveDrainTable(detached_shared->type);
            }

            sweep_lock.unlock();
            if (detached_shared)
                spillDetachedAdaptiveTable(shared, *detached_shared);
            return;
        }

        routing_type = shared.early_drain_variants->type;

        /// The estimate saturates instead of wrapping: an absurd product only means "ask for
        /// the whole budget", which the empty-budget grant still admits alone.
        size_t per_record_bytes = sizeof(UInt64) * 4 + total_size_of_aggregate_states;
        if (common::mulOverflow(batch_records, per_record_bytes, estimated_bytes)
            || common::addOverflow(estimated_bytes, batch_key_bytes, estimated_bytes))
            estimated_bytes = std::numeric_limits<size_t>::max();
    }

    /// The budget is claimed with the coordinator lock released, so a producer that must wait
    /// for a writer does not block the other producers' claims; staging is paused either way,
    /// which is the backpressure that keeps the backlog bounded under slow storage. The wait
    /// ends only with a grant or with cancellation.
    AdaptiveAggregationSession::SpillReservation reservation;
    if (!reservation.reserveOrWait(shared, estimated_bytes))
    {
        for (auto & chunk : batch)
            shared.backlog.requeue(chunk);
        return;
    }

    auto local = createAdaptiveDrainTable(routing_type);

    const size_t drained_records = StagedChunkDrainer(shared).drainBatch(*local, batch, shared.cancelled, places_scratch, StagedSliceApplier(*this));
    /// Release the staged memory before the write, not after; the batch is dropped rather
    /// than requeued even when cancellation stopped the drain early, because bucket-major
    /// progress means parts of every chunk may already be in the table.
    batch.clear();

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPressureDrainedRecords, drained_records);
    shared.backlog.recordDrained(drained_records);
    LOG_TRACE(log, "Adaptive aggregation: pressure sweep drained {} staged records into a producer-local table", drained_records);

    /// Correct the estimate upward to the built table's real footprint; never downward, so
    /// the serialization scratch still to come is not double-booked to someone else.
    reservation.resize(std::max(estimated_bytes, local->allocatedBytes()));

    if (drained_records)
        spillDetachedAdaptiveTable(shared, *local);
}

}
