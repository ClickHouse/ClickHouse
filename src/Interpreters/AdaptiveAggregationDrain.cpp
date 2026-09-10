#include <algorithm>
#include <Common/Arena.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Interpreters/AggregationUtils.h>
#include <base/unaligned.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationDrainedRecords;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

}

namespace
{
    using DB::adaptive_key_stages_bytes;

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

    /// Position and width of record j's staged key bytes. A fixed-size-key chunk carries no
    /// offsets array, and in the drains the width is the compile-time key width, so the
    /// position is a plain multiplication.
    template <typename Key>
    ALWAYS_INLINE std::pair<const char *, size_t> stagedKeyAt(const DB::StagedChunk::StagedKeys & keys, size_t j)
    {
        if constexpr (adaptive_key_stages_bytes<Key>)
            return {keys.key_bytes.data() + keys.key_offsets[j], keys.key_offsets[j + 1] - keys.key_offsets[j]};
        else
            return {keys.key_bytes.data() + j * sizeof(Key), sizeof(Key)};
    }

    /// Prefetch the table slot of the record `prefetch_look_ahead` positions ahead of `j`, if
    /// any: hash-organized tables prefetch by the saved routing hash, string tables locate the
    /// slot from the key bytes and the hash. The two drain loops share this so the dispatch
    /// cannot drift between them.
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

    /// Emplace one staged key into the table. String-like keys were staged as raw characters
    /// and are rebuilt here. `key_storage` selects the ownership: at merge time the delayed
    /// blocks are retained on the shared state until after the merged buckets are converted, so
    /// string-like keys are emplaced pointing into the staged bytes directly, with no copy; a
    /// pressure-time drain instead persists them into the arena, because freeing the blocks is
    /// its purpose. Fixed-size keys were staged as values either way.
    /// `table` is the bucket's own submap: the records were grouped by the same hash dispatch
    /// at staging time, so emplacing into it directly skips the per-record two-level routing.
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

void Aggregator::drainAdaptiveBucketForMerge(
    AggregatedDataVariants & dest,
    Arena * arena,
    size_t bucket_index,
    AdaptiveAggregationSession & shared,
    std::atomic<bool> & is_cancelled) const
{
    if (is_cancelled.load(std::memory_order_relaxed))
        return;

    const auto & backlog = shared.backlog.forMergeBucket(bucket_index);
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
            drained = drainAdaptiveBucketBacklog<AdaptiveKeyStorage::BorrowFromChunk>(
                method, arena, backlog, bucket_index, records_available, places_scratch, is_cancelled);
        });

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationDrainedRecords, drained);
    shared.backlog.recordDrained(drained);
}

/// Shared by both method kinds: the routing, the reserve sampling and the slicing are the same
/// whether or not a cell carries a mapped value.
template <AdaptiveKeyStorage key_storage, typename Method>
size_t NO_INLINE Aggregator::drainAdaptiveBucketBacklog(
    Method & method,
    Arena * arena,
    const std::vector<StagedChunkPtr> & backlog,
    size_t bucket_index,
    size_t total_records,
    PaddedPODArray<AggregateDataPtr> & places,
    std::atomic<bool> & is_cancelled) const
{
    auto & impl = method.data.impls[bucket_index];

    /// At least one record: the extrapolation below divides by the sample size.
    const size_t reserve_sample_records = std::max<size_t>(1, total_records / adaptive_reserve_sample_inverse);
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
        chassert(keys.fixed_key_size == (adaptive_key_stages_bytes<typename Method::Key> ? 0 : sizeof(typename Method::Key)));
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
            [[maybe_unused]] const auto & multiplicities = counts->multiplicities;
            for (size_t j = slice_begin; j < slice_end; ++j)
            {
                prefetchStagedKey<typename Method::Key>(impl, keys, j, slice_end);

                const auto [key_data, key_size] = stagedKeyAt<typename Method::Key>(keys, j);
                typename Method::Data::LookupResult it;
                bool inserted = false;
                emplaceStagedKey<typename Method::Key, key_storage>(
                    impl, key_data, key_size, keys.routing_hashes[j], *arena, it, inserted);

                /// A set has no counter to carry the run length into: the key being present is the
                /// whole of the record's contribution. Only the simple-count shape stages counts, and
                /// that shape has a mapped value by construction.
                if constexpr (MapAggregationMethod<Method>)
                {
                    if (inserted)
                        getInlineCountState(it->getMapped()) = multiplicities[j];
                    else
                        getInlineCountState(it->getMapped()) += multiplicities[j];
                }
            }
        }
        else
        {
            drainAdaptiveBucketImpl<key_storage>(method, arena, block, slice_begin, slice_end, places, bucket_index);
        }

        update_reserve(slice_end - slice_begin);
        drained += slice_end - slice_begin;
    }

    return drained;
}

/// The set counterpart of the bucket drain: emplacing the staged key is the whole of it. There is no
/// state to create for a new key and nothing to advance for a key already there, so the slice needs
/// neither the places nor the aggregate instructions the mapped drain builds.
template <AdaptiveKeyStorage key_storage, typename Method>
requires SetAggregationMethod<Method>
void NO_INLINE Aggregator::drainAdaptiveBucketImpl(
    Method & method,
    Arena * bucket_arena,
    const StagedChunk & block,
    size_t slice_begin,
    size_t slice_end,
    PaddedPODArray<AggregateDataPtr> &,
    size_t bucket_index) const
{
    auto & impl = method.data.impls[bucket_index];
    const auto & keys = block.keys;

    for (size_t j = slice_begin; j < slice_end; ++j)
    {
        prefetchStagedKey<typename Method::Key>(impl, keys, j, slice_end);

        const auto [key_data, key_size] = stagedKeyAt<typename Method::Key>(keys, j);
        typename Method::Data::LookupResult it;
        [[maybe_unused]] bool inserted = false;
        emplaceStagedKey<typename Method::Key, key_storage>(
            impl, key_data, key_size, keys.routing_hashes[j], *bucket_arena, it, inserted);
    }
}

template <AdaptiveKeyStorage key_storage, typename Method>
requires MapAggregationMethod<Method>
void NO_INLINE Aggregator::drainAdaptiveBucketImpl(
    Method & method,
    Arena * bucket_arena,
    const StagedChunk & block,
    size_t slice_begin,
    size_t slice_end,
    PaddedPODArray<AggregateDataPtr> & places,
    size_t bucket_index) const
{
    auto & impl = method.data.impls[bucket_index];
    const auto & keys = block.keys;

    const auto & prep = *std::get<StagedChunk::AggregatePayload>(block.payload).prepared;

    /// Conversion materializes staged arguments, and the drain assigns a state to every record.
    /// These dense columns and non-null places can use the compiled aggregate functions directly.
    chassert(!hasSparseArguments(prep.instructions.data()));
    bool use_compiled_functions = false;
#if USE_EMBEDDED_COMPILER
    use_compiled_functions = compiled_aggregate_functions_holder != nullptr;
#endif

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
            aggregate_data = bucket_arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            createAggregateStates(aggregate_data, use_compiled_functions);
            it->getMapped() = aggregate_data;
        }
        else
            aggregate_data = it->getMapped();

        places[j] = aggregate_data;
    }

    if (!params.aggregates_size)
        return;

    /// Apply the aggregate functions to the delayed rows only: the slice is a contiguous row
    /// range of the compacted argument columns and of `places`, so the standard executor
    /// applies to it directly: one compiled row loop for the compiled functions, and a batch pass
    /// per remaining function.
    executeAggregateInstructions(
        bucket_arena,
        slice_begin,
        slice_end,
        prep.instructions.data(),
        places.data(),
        /*key_start=*/slice_begin,
        /*has_only_one_value_since_last_reset=*/false,
        /*all_keys_are_const=*/false,
        use_compiled_functions);
}

size_t Aggregator::drainStagedBatch(
    AggregatedDataVariants & table,
    const std::vector<StagedChunkPtr> & chunks,
    std::atomic<bool> & is_cancelled,
    PaddedPODArray<AggregateDataPtr> & places_scratch) const
{
    size_t drained = 0;
    visitTwoLevelVariant(
        table,
        [&](auto & method)
        {
            bool no_more_keys = false;
            for (size_t b = 0; b < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++b)
            {
                if (is_cancelled.load(std::memory_order_relaxed))
                    return;

                size_t records = 0;
                for (const auto & chunk : chunks)
                    records += chunk->keys.recordsForBucket(b);
                if (!records)
                    continue;

                drained += drainAdaptiveBucketBacklog<AdaptiveKeyStorage::CopyToArena>(
                    method, table.aggregates_pools.at(b).get(), chunks, b, records, places_scratch, is_cancelled);

                /// This drain feeds the external path, which never runs the merge-time group
                /// accounting, so `max_rows_to_group_by` is held against the drain table as it
                /// grows, bucket by bucket. The table holds deduplicated keys, so its size is
                /// a lower bound on the final group count and a throw-mode crossing is
                /// definite; checking per bucket makes the query abort within one bucket's
                /// worth of records past the limit instead of after a whole floor-sized batch.
                checkLimits(table.size(), no_more_keys);
            }
        });
    return drained;
}

}
