/// The method-specialized frozen consume kernels. Hits update the local table in place; misses
/// are recorded into the block's `AdaptiveAggregationMissesInfo`, which travels with the forwarded
/// columns to the partitioning transform that builds the staged chunk.

#include <limits>
#include <Common/ProfileEvents.h>
#include <Interpreters/AdaptiveAggregationChunkInfo.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Interpreters/AggregationUtils.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationProbeBypasses;
    extern const Event AggregationOptimizedEqualRangesOfKeys;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
    extern const int LOGICAL_ERROR;
}

namespace
{
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

    /// Whether the state reads its keys straight from a `ColumnString`, exposing the column's
    /// padded `chars` and `offsets` indexed by the block row. Such keys are not copied at
    /// recording time: the producer forwards the key column and the chunk build reads the bytes
    /// from it. The low-cardinality wrapper inherits `chars` and `offsets` bound to its
    /// dictionary (rows go through `positions`), so it is excluded structurally.
    template <typename State>
    constexpr bool adaptive_key_bytes_in_column
        = requires(State & state) { state.chars; state.offsets; } && !requires(State & state) { state.positions; };

    template <typename SharedKey, typename State>
    void beginRecording(
        AdaptiveAggregationMissesInfo & misses, const AdaptiveAggregationProducer::FrozenState & frozen, bool constant_key, bool counts_only)
    {
        using KeyBytes = AdaptiveAggregationMissesInfo::KeyBytes;
        constexpr bool in_column = adaptive_key_bytes_in_column<State>;
        static_assert(!in_column || adaptive_key_stages_bytes<SharedKey>);
        constexpr KeyBytes source = in_column ? KeyBytes::InKeyColumn
            : adaptive_key_stages_bytes<SharedKey> ? KeyBytes::Recorded : KeyBytes::Fixed;
        constexpr size_t fixed_key_size = adaptive_key_stages_bytes<SharedKey> ? 0 : sizeof(SharedKey);
        misses.beginRecording(
            source, fixed_key_size, constant_key, counts_only, frozen.last_recorded_misses, frozen.last_recorded_key_bytes);
    }

    void updateProbeBypass(AdaptiveAggregationProducer::FrozenState & frozen, size_t hits, size_t rows)
    {
        if (frozen.bypass_local_probe)
            return;
        frozen.sampled_hits += hits;
        frozen.sampled_rows += rows;
        if (frozen.sampled_rows >= adaptive_bypass_sample_rows
            && frozen.sampled_hits * adaptive_bypass_hit_rate_inverse < frozen.sampled_rows)
        {
            frozen.bypass_local_probe = true;
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);
        }
    }
}

void Aggregator::executeFrozen(
    size_t row_begin,
    size_t row_end,
    AggregatedDataVariants & result,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    AdaptiveAggregationProducer & adaptive,
    AdaptiveAggregationMissesInfo & misses,
    bool all_keys_are_const) const
{
    if (row_end > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adaptive aggregation got a block of {} rows; row numbers are 32-bit.", row_end);

#define M(NAME) \
    else if (result.type == AggregatedDataVariants::Type::NAME) \
        executeFrozenImpl( \
            *result.NAME, \
            std::type_identity<std::decay_t<decltype(*result.NAME##_two_level)>>{}, \
            result.aggregates_pool, \
            row_begin, \
            row_end, \
            key_columns, \
            aggregate_instructions, \
            adaptive, \
            misses, \
            all_keys_are_const);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
    else
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant in the adaptive frozen path.");
}

template <typename LocalMethod, typename SharedMethod>
requires SetAggregationMethod<LocalMethod>
void NO_INLINE Aggregator::executeFrozenImpl(
    LocalMethod & local_method,
    std::type_identity<SharedMethod>,
    Arena *,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction *,
    AdaptiveAggregationProducer & adaptive,
    AdaptiveAggregationMissesInfo & misses,
    bool all_keys_are_const) const
{
    static_assert(SharedMethod::Data::NUM_BUCKETS == ADAPTIVE_AGGREGATION_NUM_BUCKETS);
    using State = typename LocalMethod::StateNoCache;
    using SharedKey = typename SharedMethod::Key;
    constexpr bool key_in_column = adaptive_key_bytes_in_column<State>;
    chassert(key_in_column == adaptive_key_column_position.has_value());

    Arena scratch_pool;
    State local_find_state(key_columns, key_sizes, aggregation_state_cache);

    /// The kernel runs only while the producer is frozen, and phase transitions happen between
    /// blocks, so the reference stays valid for the whole block.
    auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    const bool bypass_local_probe = frozen.bypass_local_probe;
    beginRecording<SharedKey, State>(misses, frozen, all_keys_are_const, /*counts_only=*/false);

    /// `executeFrozen` pairs the local method with its own two-level form, which has the same
    /// hash function and a static bucket mapping. Routing uses that type without reading the
    /// shared table, which a pressure spill can replace concurrently.
    const auto record_miss = [&](const auto & key, UInt64 hash, size_t row)
    {
        misses.recordMiss<SharedKey, key_in_column>(
            static_cast<UInt32>(row), hash, static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash)), key);
    };

    if (all_keys_are_const)
    {
        auto && key_holder = local_find_state.getKeyHolder(0, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);
        /// The whole range carries one key and a set stores a key once, so a single record stands
        /// for it however many rows it spans.
        if (!local_method.data.find(key, hash))
            record_miss(key, hash, row_begin);
        keyHolderDiscardKey(key_holder);
        return;
    }

    size_t hits = 0;
    for (size_t i = row_begin; i < row_end; ++i)
    {
        auto && key_holder = local_find_state.getKeyHolder(i, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);
        if (!bypass_local_probe && local_method.data.find(key, hash))
            ++hits;
        else
            record_miss(key, hash, i);
        keyHolderDiscardKey(key_holder);
    }
    updateProbeBypass(frozen, hits, row_end - row_begin);
}

template <typename LocalMethod, typename SharedMethod>
requires MapAggregationMethod<LocalMethod>
void NO_INLINE Aggregator::executeFrozenImpl(
    LocalMethod & local_method,
    std::type_identity<SharedMethod>,
    Arena * aggregates_pool,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    AdaptiveAggregationProducer & adaptive,
    AdaptiveAggregationMissesInfo & misses,
    bool all_keys_are_const) const
{
    static_assert(SharedMethod::Data::NUM_BUCKETS == ADAPTIVE_AGGREGATION_NUM_BUCKETS);
    using State = typename LocalMethod::StateNoCache;
    using SharedKey = typename SharedMethod::Key;
    constexpr bool key_in_column = adaptive_key_bytes_in_column<State>;
    chassert(key_in_column == adaptive_key_column_position.has_value());

    Arena scratch_pool;
    State local_find_state(key_columns, key_sizes, aggregation_state_cache);

    /// The kernel runs only while the producer is frozen, and phase transitions happen between
    /// blocks, so the reference stays valid for the whole block.
    auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    const bool bypass_local_probe = frozen.bypass_local_probe;
    beginRecording<SharedKey, State>(misses, frozen, all_keys_are_const, is_simple_count);

    /// `executeFrozen` pairs the local method with its own two-level form, which has the same
    /// hash function and a static bucket mapping. Routing uses that type without reading the
    /// shared table, which a pressure spill can replace concurrently.
    const auto bucket_of = [](UInt64 hash) { return static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash)); };
    const auto record_miss = [&](const auto & key, UInt64 hash, size_t row)
    {
        misses.recordMiss<SharedKey, key_in_column>(static_cast<UInt32>(row), hash, bucket_of(hash), key);
    };

    if (all_keys_are_const)
    {
        auto && key_holder = local_find_state.getKeyHolder(0, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);
        if (auto it = local_method.data.find(key, hash))
        {
            if (is_simple_count)
                getInlineCountState(it->getMapped()) += row_end - row_begin;
            else
            {
                /// Apply the whole range to the single place, mirroring the ordinary
                /// all-keys-are-const handling.
                for (size_t i = 0; i < aggregate_functions.size(); ++i)
                {
                    AggregateFunctionInstruction * inst = aggregate_instructions + i;
                    ProfileEvents::increment(ProfileEvents::AggregationOptimizedEqualRangesOfKeys);
                    addBatchSinglePlace(row_begin, row_end, inst, it->getMapped() + inst->state_offset, aggregates_pool);
                }
            }
        }
        else if (is_simple_count)
        {
            /// One count run stands for the whole range.
            misses.recordCountRun<SharedKey, key_in_column>(
                static_cast<UInt32>(row_begin), hash, bucket_of(hash), key, static_cast<UInt32>(row_end - row_begin));
        }
        else
        {
            for (size_t i = row_begin; i < row_end; ++i)
                record_miss(key, hash, i);
        }
        keyHolderDiscardKey(key_holder);
        return;
    }

    if (is_simple_count)
    {
        /// Consecutive misses of one key collapse into one run while the key is still at hand,
        /// so the chunk build never re-walks the records to find them.
        size_t hits = 0;
        SharedKey last_staged_key{};
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

            const SharedKey staged_key = key;

            bool run_continues = misses.lastCountRunHasHash(hash);
            if constexpr (std::is_same_v<SharedKey, std::string_view>)
                run_continues = run_continues && stable_key_views && staged_key == last_staged_key;
            else
                run_continues = run_continues && staged_key == last_staged_key;

            if (run_continues)
            {
                misses.extendLastCountRun();
            }
            else
            {
                misses.recordCountRun<SharedKey, key_in_column>(static_cast<UInt32>(i), hash, bucket_of(hash), staged_key, 1);

                /// A serialized key view points into the reused scratch arena and can only seed
                /// the run tracking when the views are block-stable; every other key type is
                /// either a self-contained value or, for a packed reference, points into the
                /// block's key column, whose bytes outlive the block.
                if constexpr (std::is_same_v<SharedKey, std::string_view>)
                {
                    if (stable_key_views)
                        last_staged_key = staged_key;
                }
                else
                {
                    last_staged_key = staged_key;
                }
            }
            keyHolderDiscardKey(key_holder);
        }
        updateProbeBypass(frozen, hits, row_end - row_begin);
        return;
    }

    /// The probe/recording loop, shared by the with-places and the zero-aggregates shapes: a
    /// keyed GROUP BY without aggregate functions needs no places at all (the baseline
    /// specializes the same way), so it skips the allocation and the per-row stores.
    const auto probe_rows = [&]<bool record_places>(AggregateDataPtr * places_data) -> size_t
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
            record_miss(key, hash, i);
            keyHolderDiscardKey(key_holder);
        }
        return hits;
    };

    if (params.aggregates_size == 0)
    {
        const size_t hits = probe_rows.template operator()<false>(nullptr);
        updateProbeBypass(frozen, hits, row_end - row_begin);
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
    updateProbeBypass(frozen, hits, row_end - row_begin);

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

}
