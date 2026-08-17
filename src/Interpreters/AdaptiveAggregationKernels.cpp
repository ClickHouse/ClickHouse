/// The method-specialized kernels of the adaptive aggregation: the frozen consume path, the
/// staged-chunk builds, and the bucket drains. They are member templates of `Aggregator` and
/// `StagedChunkBuilder` (defined here rather than next to their classes, following
/// `ClientBaseOptimizedParts.cpp`), dispatched over the aggregation-method variants.

#include <bit>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/MemoryTrackerUtils.h>
#include <base/arithmeticOverflow.h>
#include <base/unaligned.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Interpreters/AggregationUtils.h>

namespace ProfileEvents
{
    extern const Event AggregationOptimizedEqualRangesOfKeys;
    extern const Event AdaptiveAggregationThaws;
    extern const Event AdaptiveAggregationProbeBypasses;
    extern const Event AdaptiveAggregationStagedRecords;
    extern const Event AdaptiveAggregationStagedRecordsMerged;
    extern const Event AdaptiveAggregationStagedBytes;
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
    template <typename Key>
    ALWAYS_INLINE std::string_view adaptiveStagedKeyBytes(const Key & key)
    {
        if constexpr (std::is_same_v<Key, PackedStringRef>)
            return static_cast<std::string_view>(key);
        else
            return key;
    }

    /// Runs `callback` on the row's key bytes while their owner is alive. This is the only
    /// safe shape: a generic hashing state's key holder may own the bytes itself (an
    /// exact-size allocation) or roll its scratch-arena allocation back on discard, so a
    /// pointer must not outlive the holder. States that expose their padded column buffers
    /// skip the holder entirely; fixed-size keys are copied into a local first. The padding
    /// in the ref tells the callback which comparison and copy primitives are legal.
    template <typename SharedKey, typename State, typename Callback>
    void ALWAYS_INLINE withStagedKeyBytes(State & state, size_t row, size_t size, DB::Arena & scratch, Callback && callback)
    {
        /// The fast path requires buffers indexed by the block row directly; the low-cardinality
        /// wrapper inherits `chars`/`offsets` bound to its dictionary (rows go through
        /// `positions`), so it is excluded structurally rather than left to the admission gate.
        if constexpr (requires { state.chars; state.offsets; } && !requires { state.positions; })
        {
            const char * data
                = reinterpret_cast<const char *>(state.chars) + state.offsets[static_cast<ssize_t>(row) - 1];
            callback(DB::KeyBytesRef{std::string_view(data, size), DB::ReadablePadding::AtLeast15Bytes});
        }
        else if constexpr (DB::adaptive_key_stages_bytes<SharedKey>)
        {
            auto && key_holder = state.getKeyHolder(row, scratch);
            callback(DB::KeyBytesRef{adaptiveStagedKeyBytes(keyHolderGetKey(key_holder)), DB::ReadablePadding::Exact});
            keyHolderDiscardKey(key_holder);
        }
        else
        {
            auto && key_holder = state.getKeyHolder(row, scratch);
            const SharedKey widened = keyHolderGetKey(key_holder);
            keyHolderDiscardKey(key_holder);
            callback(DB::KeyBytesRef{std::string_view(reinterpret_cast<const char *>(&widened), sizeof(widened)), DB::ReadablePadding::Exact});
        }
    }

    /// Whether the string views the state's key holders hand out point into storage that
    /// outlives the row loop (a batch-serialized buffer or the key column itself) rather than
    /// into per-row scratch that dies with the holder. Only the serialized methods can go
    /// either way, and they expose the choice as `use_batch_serialize`; the run tracking in
    /// the count kernel may only remember a previous key's view when this holds.
    template <typename State>
    bool ALWAYS_INLINE adaptiveKeyViewsAreBlockStable(const State & state)
    {
        if constexpr (requires { state.use_batch_serialize; })
            return state.use_batch_serialize;
        else
            return true;
    }
}

namespace DB
{

void Aggregator::executeFrozen(
    const Columns & columns,
    size_t row_begin,
    size_t row_end,
    AggregatedDataVariants & result,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    AdaptiveAggregationProducer & adaptive,
    bool all_keys_are_const) const
{
#define M(NAME) \
    else if (result.type == AggregatedDataVariants::Type::NAME) \
        executeFrozenImpl( \
            *result.NAME, \
            std::type_identity<std::decay_t<decltype(*result.NAME##_two_level)>>{}, \
            result.aggregates_pool, \
            columns, \
            row_begin, \
            row_end, \
            key_columns, \
            aggregate_instructions, \
            adaptive, \
            all_keys_are_const);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
    else
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant in the adaptive frozen path.");
}

template <typename LocalMethod, typename SharedMethod>
void NO_INLINE Aggregator::executeFrozenImpl(
    LocalMethod & local_method,
    std::type_identity<SharedMethod>,
    Arena * aggregates_pool,
    const Columns & columns,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    AdaptiveAggregationProducer & adaptive,
    bool all_keys_are_const) const
{
    Arena scratch_pool;

    typename LocalMethod::StateNoCache local_find_state(key_columns, key_sizes, aggregation_state_cache);
    /// Routing needs only the two-level twin's TYPE: the hash is the local table's canonical
    /// hash (identical to the twin's by construction - the pairing in `executeFrozen` binds a
    /// method to its own two-level form, which keeps the hash function), and the bucket
    /// mapping is static. Borrowing the shared table's method instance here would race with a
    /// pressure spill re-initializing it; the mutable early-drain table must not double as a
    /// hash-policy object.

    /// The kernel runs only while the producer is frozen, and phase transitions happen between
    /// blocks, so the reference stays valid for the whole block.
    auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    auto update_bypass_sampling = [&](size_t hits, size_t rows)
    {
        if (frozen.recordProbeSample(hits, rows))
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);
    };
    const bool bypass_local_probe = frozen.bypass_local_probe;

    if (all_keys_are_const)
    {
        auto && key_holder = local_find_state.getKeyHolder(0, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);

        bool found = false;
        AggregateDataPtr found_place = nullptr;
        if (auto it = local_method.data.find(key, hash))
        {
            found = true;
            if (is_simple_count)
                getInlineCountState(it->getMapped()) += row_end - row_begin;
            else
                found_place = it->getMapped();
        }

        if (found)
        {
            if (!is_simple_count && params.aggregates_size)
            {
                /// Apply the whole range to the single place, mirroring the ordinary
                /// all-keys-are-const handling.
                for (size_t i = 0; i < aggregate_functions.size(); ++i)
                {
                    AggregateFunctionInstruction * inst = aggregate_instructions + i;
                    ProfileEvents::increment(ProfileEvents::AggregationOptimizedEqualRangesOfKeys);
                    addBatchSinglePlace(row_begin, row_end, inst, found_place + inst->state_offset, aggregates_pool);
                }
            }
        }
        else
        {
            const auto bucket = static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash));

            if (is_simple_count)
            {
                adaptive.staging.misses.hashes.push_back(hash);
                adaptive.staging.misses.multiplicities.push_back(static_cast<UInt32>(row_end - row_begin));
                if constexpr (DB::adaptive_key_stages_bytes<typename SharedMethod::Key>)
                    adaptive.staging.misses.key_sizes.push_back(adaptiveStagedKeyBytes(key).size());
                adaptive.staging.misses.buckets.push_back(bucket);
            }
            else
            {
                for (size_t i = row_begin; i < row_end; ++i)
                {
                    adaptive.staging.misses.source_rows.push_back(static_cast<UInt32>(i));
                    adaptive.staging.misses.hashes.push_back(hash);
                    adaptive.staging.misses.buckets.push_back(bucket);
                    if constexpr (DB::adaptive_key_stages_bytes<typename SharedMethod::Key>)
                        adaptive.staging.misses.key_sizes.push_back(adaptiveStagedKeyBytes(key).size());
                }
            }
            stageRecordedMisses<typename SharedMethod::Key>(
                columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/is_simple_count, /*key_row_override=*/0);
        }
        keyHolderDiscardKey(key_holder);
        return;
    }

    if (is_simple_count)
    {
        size_t hits = 0;
        typename SharedMethod::Key last_staged_key{};
        [[maybe_unused]] const bool stable_key_views = adaptiveKeyViewsAreBlockStable(local_find_state);
        for (size_t i = row_begin; i < row_end; ++i)
        {
            auto && key_holder = local_find_state.getKeyHolder(i, scratch_pool);
            const auto & key = keyHolderGetKey(key_holder);
            const UInt64 hash = local_method.data.hash(key);

            if (!bypass_local_probe)
            {
                if (auto it = local_method.data.find(key, hash))
                {
                    ++hits;
                    ++getInlineCountState(it->getMapped());
                    keyHolderDiscardKey(key_holder);
                    continue;
                }
            }

            const typename SharedMethod::Key staged_key = key;

            bool run_continues = !adaptive.staging.misses.hashes.empty() && adaptive.staging.misses.hashes.back() == hash;
            if constexpr (std::is_same_v<typename SharedMethod::Key, std::string_view>)
                run_continues = run_continues && stable_key_views && staged_key == last_staged_key;
            else
                run_continues = run_continues && staged_key == last_staged_key;

            if (run_continues)
            {
                ++adaptive.staging.misses.multiplicities.back();
            }
            else
            {
                adaptive.staging.misses.hashes.push_back(hash);
                adaptive.staging.misses.multiplicities.push_back(1);
                /// Fixed-size keys stage no size: it is a compile-time constant the publish
                /// substitutes, so the hot staging loop skips a dead store per record.
                if constexpr (DB::adaptive_key_stages_bytes<typename SharedMethod::Key>)
                    adaptive.staging.misses.key_sizes.push_back(adaptiveStagedKeyBytes(staged_key).size());

                /// A serialized key view points into the reused scratch arena and can only seed
                /// the run tracking when the views are block-stable; every other key type is
                /// either a self-contained value or, for a packed reference, points into the
                /// block's key column, whose bytes outlive the block.
                if constexpr (std::is_same_v<typename SharedMethod::Key, std::string_view>)
                {
                    if (stable_key_views)
                        last_staged_key = staged_key;
                }
                else
                {
                    last_staged_key = staged_key;
                }
                adaptive.staging.misses.source_rows.push_back(static_cast<UInt32>(i));
                adaptive.staging.misses.buckets.push_back(static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash)));
            }
            keyHolderDiscardKey(key_holder);
        }
        update_bypass_sampling(hits, row_end - row_begin);
        stageRecordedMisses<typename SharedMethod::Key>(columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/true);
        return;
    }

    /// The probe/staging loop, shared by the with-places and the zero-aggregates shapes: a
    /// keyed GROUP BY without aggregate functions needs no places at all (the baseline
    /// specializes the same way), so it skips the allocation and the per-row stores.
    auto probe_rows = [&]<bool record_places>(AggregateDataPtr * places_data) -> size_t
    {
        size_t hits = 0;
        for (size_t i = row_begin; i < row_end; ++i)
        {
            auto && key_holder = local_find_state.getKeyHolder(i, scratch_pool);
            const auto & key = keyHolderGetKey(key_holder);
            const UInt64 hash = local_method.data.hash(key);

            if (!bypass_local_probe)
            {
                if (auto it = local_method.data.find(key, hash))
                {
                    ++hits;
                    if constexpr (record_places)
                        places_data[i] = it->getMapped();
                    keyHolderDiscardKey(key_holder);
                    continue;
                }
            }

            if constexpr (record_places)
                places_data[i] = nullptr;
            adaptive.staging.misses.source_rows.push_back(static_cast<UInt32>(i));
            adaptive.staging.misses.hashes.push_back(hash);
            adaptive.staging.misses.buckets.push_back(static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash)));

            if constexpr (DB::adaptive_key_stages_bytes<typename SharedMethod::Key>)
                adaptive.staging.misses.key_sizes.push_back(adaptiveStagedKeyBytes(key).size());
            keyHolderDiscardKey(key_holder);
        }
        return hits;
    };

    if (params.aggregates_size == 0)
    {
        const size_t hits = probe_rows.template operator()<false>(nullptr);
        update_bypass_sampling(hits, row_end - row_begin);
        stageRecordedMisses<typename SharedMethod::Key>(columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);
        return;
    }

    AllocatorWithMemoryTracking<AggregateDataPtr> allocator;
    const size_t places_size = row_end;
    auto places_deleter = [&allocator, &places_size](auto * ptr)
    {
        if (ptr) [[likely]]
            allocator.deallocate(ptr, places_size);
    };
    std::unique_ptr<AggregateDataPtr[], decltype(places_deleter)> places(allocator.allocate(places_size), places_deleter);

    const size_t hits = probe_rows.template operator()<true>(places.get());
    update_bypass_sampling(hits, row_end - row_begin);
    stageRecordedMisses<typename SharedMethod::Key>(columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);

    /// With no local hits every place is null and the batch pass would only skip rows; the
    /// staged records carry the block's whole contribution. Bypassed blocks are always all-miss.
    if (hits != 0)
        executeAggregateInstructions(
            aggregates_pool,
            row_begin,
            row_end,
            aggregate_instructions,
            places.get(),
            /*key_start=*/row_begin,
            /*has_only_one_value_since_last_reset=*/false,
            /*all_keys_are_const=*/false,
            /*use_compiled_functions=*/false);
}

template <typename SharedKey, typename State>
void NO_INLINE StagedChunkBuilder::buildCountChunk(
    StagedChunk & chunk,
    State & local_find_state,
    Arena & scratch_pool,
    std::optional<UInt32> key_row_override)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;
    const size_t total = misses.hashes.size();

    /// Group the records by (bucket, a few extra hash bits): a duplicate key always lands in
    /// the same group, so the dedup below only compares within a group, and group-id order is
    /// bucket-major, which is the block's slice layout. The group count scales with the batch
    /// (~16 records per group), so the histogram stays cache-resident and small batches do not
    /// pay for counters they cannot fill. A bypassed pass (see `DedupProductivity`) degrades
    /// the grouping to plain buckets and the dedup scan below to a straight append.
    const bool dedup = build_dedup.shouldDedup();
    const UInt32 sub_bits = dedup ? std::min<UInt32>(8, std::bit_width(total >> 12)) : 0;
    const size_t num_groups = num_buckets << sub_bits;

    auto & grouped_indexes = grouped_index_scratch;
    grouped_indexes.resize(total);
    auto & offsets = group_offsets_scratch;
    auto & cursor = group_cursor_scratch;
    offsets.assign(num_groups + 1, 0);
    cursor.resize(num_groups);

    const auto group_of = [&](size_t i) -> UInt32
    {
        const UInt32 bucket = misses.buckets[i];
        return (bucket << sub_bits) | (static_cast<UInt32>(misses.hashes[i] >> 10) & ((1u << sub_bits) - 1));
    };

    for (size_t i = 0; i < total; ++i)
        ++offsets[group_of(i) + 1];
    for (size_t g = 0; g < num_groups; ++g)
    {
        cursor[g] = offsets[g];
        offsets[g + 1] += offsets[g];
    }
    for (size_t i = 0; i < total; ++i)
        grouped_indexes[cursor[group_of(i)]++] = static_cast<UInt32>(i);

    /// Fixed-size keys stage no per-record size (see the kernels); the publish substitutes the
    /// compile-time constant.
    UInt64 total_bytes = 0;
    if constexpr (DB::adaptive_key_stages_bytes<SharedKey>)
        for (const auto size : misses.key_sizes)
            total_bytes += size;
    else
        total_bytes = total * sizeof(SharedKey);

    auto & keys = chunk.keys;
    auto & multiplicities = chunk.payload.emplace<StagedChunk::CountPayload>().multiplicities;
    if constexpr (!DB::adaptive_key_stages_bytes<SharedKey>)
        keys.fixed_key_size = sizeof(SharedKey);
    keys.routing_hashes.resize(total);
    multiplicities.resize(total);
    if constexpr (DB::adaptive_key_stages_bytes<SharedKey>)
        keys.key_offsets.resize(total + 1);
    keys.key_bytes.resize(total_bytes);

    size_t out = 0;
    UInt64 byte_pos = 0;
    for (size_t g = 0; g < num_groups; ++g)
    {
        if ((g & ((1u << sub_bits) - 1)) == 0)
            keys.bucket_offsets[g >> sub_bits] = static_cast<UInt32>(out);
        const size_t group_begin = offsets[g];
        const size_t group_end = offsets[g + 1];
        if (group_begin == group_end)
            continue;

        const size_t group_out_begin = out;
        for (size_t i = group_begin; i < group_end; ++i)
        {
            const auto idx = grouped_indexes[i];
            const UInt64 hash = misses.hashes[idx];
            const size_t size = [&]
            {
                if constexpr (DB::adaptive_key_stages_bytes<SharedKey>)
                    return misses.key_sizes[idx];
                else
                    return sizeof(SharedKey);
            }();
            const size_t key_row = key_row_override ? *key_row_override : misses.source_rows[idx];

            /// The key bytes are read straight from the hashing state's column when it exposes
            /// them: the generic key holder of the packed method would re-pack the key and
            /// re-compute its content hash per record, and the staged arrays already hold both.
            /// All byte uses happen inside the holder's lifetime (see `withStagedKeyBytes`).
            /// A bypassed pass hands the append an empty candidate range, so nothing is scanned.
            /// The lambda must inline: outlined, it would cost a call and a closure spill per
            /// staged record in this loop.
            withStagedKeyBytes<SharedKey>(
                local_find_state,
                key_row,
                size,
                scratch_pool,
                [&](const KeyBytesRef & key) ALWAYS_INLINE
                {
                    mergeOrAppendStagedCount(
                        keys, multiplicities, hash, key, misses.multiplicities[idx], dedup ? group_out_begin : out, out, byte_pos);
                });
        }
    }

    keys.bucket_offsets[num_buckets] = static_cast<UInt32>(out);
    if constexpr (DB::adaptive_key_stages_bytes<SharedKey>)
    {
        keys.key_offsets[out] = byte_pos;
        keys.key_offsets.resize(out + 1);
    }

    keys.routing_hashes.resize(out);
    multiplicities.resize(out);
    keys.key_bytes.resize(byte_pos);

    if (dedup)
        build_dedup.record(total, out);
}

template <typename SharedKey, typename State>
void NO_INLINE StagedChunkBuilder::buildAggregateChunk(
    StagedChunk & chunk,
    const Columns & columns,
    State & local_find_state,
    Arena & scratch_pool,
    std::optional<UInt32> key_row_override)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;
    const size_t total = misses.hashes.size();
    auto & keys = chunk.keys;

    auto & payload = chunk.payload.emplace<StagedChunk::AggregatePayload>();

    /// The sizes are exact and final, and the chunk can sit on a backlog for the rest of the
    /// query, so the arrays are sized without the power-of-two growth headroom.
    keys.routing_hashes.resize_exact(total);
    if constexpr (DB::adaptive_key_stages_bytes<SharedKey>)
        keys.key_offsets.resize_exact(total + 1);
    else
        keys.fixed_key_size = sizeof(SharedKey);

    /// Counting sort of the staged misses by bucket: one pass over the records accumulates the
    /// record and key-byte histograms together, one pass over the buckets turns both into
    /// exclusive offsets.
    /// Fixed-size keys stage no per-record size (see the kernels); the publish substitutes the
    /// compile-time constant.
    const auto staged_key_size = [&](size_t record)
    {
        if constexpr (DB::adaptive_key_stages_bytes<SharedKey>)
            return misses.key_sizes[record];
        else
            return sizeof(SharedKey);
    };

    std::array<UInt32, num_buckets> cursor{};
    std::array<UInt64, num_buckets> byte_cursor{};
    for (size_t i = 0; i < total; ++i)
    {
        ++cursor[misses.buckets[i]];
        byte_cursor[misses.buckets[i]] += staged_key_size(i);
    }

    UInt32 offset = 0;
    UInt64 byte_offset = 0;
    for (size_t b = 0; b < num_buckets; ++b)
    {
        keys.bucket_offsets[b] = offset;
        const auto count = cursor[b];
        cursor[b] = offset;
        offset += count;

        const auto bytes = byte_cursor[b];
        byte_cursor[b] = byte_offset;
        byte_offset += bytes;
    }
    keys.bucket_offsets[num_buckets] = offset;
    if constexpr (DB::adaptive_key_stages_bytes<SharedKey>)
        keys.key_offsets[total] = byte_offset;

    keys.key_bytes.resize_exact(byte_offset);

    /// The records' source row numbers in bucket-grouped order: the gather indexes that compact
    /// the argument columns below. A zero-aggregate block stages keys only, so it needs none.
    ColumnUInt32::MutablePtr gather_indexes;
    UInt32 * gather_data = nullptr;
    if (aggregates_size != 0)
    {
        gather_indexes = ColumnUInt32::create();
        gather_indexes->getData().resize_exact(total);
        gather_data = gather_indexes->getData().data();
    }

    for (size_t i = 0; i < total; ++i)
    {
        const auto b = misses.buckets[i];
        const auto pos = cursor[b]++;
        keys.routing_hashes[pos] = misses.hashes[i];

        if (gather_data)
            gather_data[pos] = misses.source_rows[i];

        const auto size = staged_key_size(i);
        const auto byte_pos = byte_cursor[b];
        byte_cursor[b] += size;
        if constexpr (DB::adaptive_key_stages_bytes<SharedKey>)
            keys.key_offsets[pos] = byte_pos;

        /// The same byte extraction the count path uses: states that expose their padded
        /// column buffers hand the bytes out directly (in particular, the packed-string method
        /// does not rebuild the key, which would re-hash its content per record). The copy is
        /// a plain bounded memcpy, NOT copyStagedKeyBytes: records scatter into bucket-grouped
        /// positions, so an overflow-tolerant write would stomp neighbors that are already in
        /// place. The guard also keeps the empty packed key's null data pointer away from
        /// memcpy, which declares its sources nonnull.
        const size_t key_row = key_row_override ? *key_row_override : misses.source_rows[i];
        withStagedKeyBytes<SharedKey>(
            local_find_state,
            key_row,
            size,
            scratch_pool,
            [&](const KeyBytesRef & key) ALWAYS_INLINE
            {
                if (!key.bytes.empty())
                    memcpy(keys.key_bytes.data() + byte_pos, key.bytes.data(), key.bytes.size());
            });
    }

    payload.argument_columns.assign(columns.size(), nullptr);
    for (const auto & argument_positions : aggregates_positions)
        for (const auto position : argument_positions)
        {
            if (payload.argument_columns[position])
                continue;
            /// A constant argument stays constant: resizing it to the record count is
            /// exact and avoids materializing the whole block just to gather from it.
            /// A sparse argument is materialized before the gather: the staged rows are
            /// an arbitrary subset of the block, and the drain applies plain dense
            /// batches to them, so nothing downstream wants the sparse representation.
            if (isColumnConst(*columns[position]))
                payload.argument_columns[position] = columns[position]->cloneResized(total);
            else
                payload.argument_columns[position] = recursiveRemoveSparse(columns[position]->getPtr())->index(*gather_indexes, 0);
        }
}

template <typename SharedKey, typename State>
void Aggregator::stageRecordedMisses(
    const Columns & columns,
    size_t num_rows,
    AdaptiveAggregationProducer & adaptive,
    State & local_find_state,
    Arena & scratch_pool,
    bool counts_only,
    std::optional<UInt32> key_row_override) const
{
    const auto & miss_hashes = adaptive.staging.misses.hashes;
    const size_t total = miss_hashes.size();
    if (!total)
        return;

    /// The thaw rule: the verdict takes effect at every thread's next block, through the
    /// ordinary dispatch on the per-thread flags. The current records are still staged: their
    /// rows were deferred by the frozen kernel and only the drain will aggregate them.
    if (adaptive.session->thaw_sampler.fold(miss_hashes))
    {
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationThaws);
        LOG_TRACE(log, "Adaptive aggregation: thawing the local tables after the staged stream proved repeat-dominated");
    }

    adaptive.staging.stageMisses<SharedKey>(
        columns, num_rows, local_find_state, scratch_pool, counts_only, key_row_override, *adaptive.staging_sink);
}

template <typename SharedKey, typename State>
void NO_INLINE StagedChunkBuilder::stageMisses(
    const Columns & columns,
    size_t num_rows,
    State & local_find_state,
    Arena & scratch_pool,
    bool counts_only,
    std::optional<UInt32> key_row_override,
    IStagedChunkSink & sink)
{
    const size_t total = misses.hashes.size();
    if (!total)
        return;

    if (num_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adaptive aggregation got a block of {} rows; row numbers are 32-bit.", num_rows);

    auto chunk = std::make_shared<StagedChunk>();
    auto & keys = chunk->keys;

    if (counts_only)
    {
        buildCountChunk<SharedKey>(*chunk, local_find_state, scratch_pool, key_row_override);
    }
    else
    {
        buildAggregateChunk<SharedKey>(*chunk, columns, local_find_state, scratch_pool, key_row_override);
    }

    misses.clear();

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecords, total);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecordsMerged, total - keys.size());
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedBytes, keys.key_bytes.size());

    size_t estimated_payload_bytes
        = keys.key_bytes.size() + keys.key_offsets.size() * sizeof(UInt64) + keys.routing_hashes.size() * sizeof(UInt64);
    if (const auto * counts = std::get_if<StagedChunk::CountPayload>(&chunk->payload))
        estimated_payload_bytes += counts->multiplicities.size() * sizeof(UInt32);
    else
        for (const auto & column : std::get<StagedChunk::AggregatePayload>(chunk->payload).argument_columns)
            if (column)
                estimated_payload_bytes += column->byteSize();

    stageBuiltChunk(std::move(chunk), estimated_payload_bytes, sink);
}

}
