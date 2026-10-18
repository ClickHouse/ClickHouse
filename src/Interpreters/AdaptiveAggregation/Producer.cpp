/// The producer kernels of the adaptive aggregation: the frozen consume path and the staging of missed rows as
/// partition records. They are member templates of `Aggregator` (defined here rather than in `Aggregator.cpp`,
/// following `ClientBaseOptimizedParts.cpp`), dispatched over the aggregation-method variants.

#include <algorithm>
#include <limits>
#include <type_traits>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/memcpySmall.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregationImpl.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/RowDataStore.h>

namespace ProfileEvents
{
    extern const Event AggregationOptimizedEqualRangesOfKeys;
    extern const Event AdaptiveAggregationProbeBypasses;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

namespace
{
    /// Whether the kernel of `State` may keep the bytes of its missed string-like keys in the batch (see
    /// `AdaptiveMissBatch`): a state with `getKeyBytes` reads them from the block's padded column buffers, so it never
    /// keeps them. The probe loops check the bound on the kept bytes only under this: the check stages the batch, and a
    /// probe lambda that merely names the staging captures it, which changes the code of every other state's loop.
    template <typename RecordKey, typename State>
    constexpr bool adaptive_misses_may_keep_key_bytes
        = adaptive_key_stages_bytes<RecordKey> && !requires(const State & state) { state.getKeyBytes(size_t{}); };

    /// Whether the kernel of `state` keeps the bytes of its missed string-like keys in the batch. A state whose key
    /// views are block-stable, which the batch serialization of the serialized methods guarantees, returns the bytes
    /// that the probe read. Any other state builds a key again for every read, which for the serialized methods means
    /// serializing it a second time, so it keeps them. The states that never keep the bytes get `std::false_type`,
    /// which stays a constant in the lambdas that capture it, so their probe loops and writer compile without the kept
    /// bytes.
    template <typename RecordKey, typename State>
    auto ALWAYS_INLINE adaptiveMissesKeepKeyBytes(const State & state)
    {
        if constexpr (adaptive_misses_may_keep_key_bytes<RecordKey, State>)
            return !state.keyViewsAreBlockStable();
        else
            return std::false_type{};
    }

    /// Whether a batch that keeps the bytes of its keys holds as many of them as it may, so that the kernel stages it
    /// before it adds more misses (see `adaptive_miss_batch_max_key_bytes`).
    bool ALWAYS_INLINE missBatchKeyBytesFull(const AdaptiveMissBatch & misses, bool keep_key_bytes)
    {
        return keep_key_bytes && misses.getKeys().size() >= adaptive_miss_batch_max_key_bytes;
    }

    /// Copies the bytes of a string-like key into a staged record from a buffer that keeps padding past them: the
    /// block's key column, or the bytes that the batch of misses kept. The overflow-tolerant small copy reads and
    /// writes up to 15 bytes past the end. The records are appended in increasing byte order with every chunk ending in
    /// unused padding, so the scribble lands in space that the next append overwrites or that no record uses.
    void ALWAYS_INLINE copyPaddedKeyBytes(const char * data, size_t size, char * staged)
    {
        if (size <= 64)
            memcpySmallAllowReadWriteOverflow15(staged, data, size);
        else
            memcpy(staged, data, size);
    }

    /// Copies a string-like key of the block into a staged record. A state that exposes its padded column buffers is
    /// read directly, with `copyPaddedKeyBytes`. Another state's key holder may own the bytes in an exact-size
    /// allocation or roll its scratch-arena allocation back on discard, so its bytes are copied exactly, while the
    /// holder is alive.
    template <typename State>
    void ALWAYS_INLINE copyStagedKeyBytes(State & state, size_t row, size_t size, DB::Arena & scratch, char * staged)
    {
        if (!size)
            return;
        if constexpr (requires { state.getKeyBytes(row); })
        {
            const char * data = state.getKeyBytes(row).data();
            copyPaddedKeyBytes(data, size, staged);
        }
        else
        {
            auto && key_holder = state.getKeyHolder(row, scratch);
            const auto key = static_cast<std::string_view>(keyHolderGetKey(key_holder));
            chassert(key.size() == size);
            memcpy(staged, key.data(), size);
            keyHolderDiscardKey(key_holder);
        }
    }

    /// Where the staging reads an aggregate argument of a batch of misses: `rows[i]` is the row of `column` that record
    /// `i` reads.
    struct ArgumentSource
    {
        const IColumn * column;
        const UInt32 * rows;
    };

}

void Aggregator::executeFrozen(
    const Columns & columns,
    size_t row_begin,
    size_t row_end,
    AggregatedDataVariants & result,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    AdaptiveAggregationProducer & adaptive,
    bool all_keys_are_const,
    MemoryTracker * local_memory_tracker) const
{
    /// The rows of the block are the base of the thaw's staged share (see `adaptiveStagingRepeats`).
    std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase).rows += row_end - row_begin;

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
            all_keys_are_const, \
            local_memory_tracker);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
    else
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant in the adaptive frozen path.");
}

/// The set counterpart of the frozen kernel. A `GROUP BY` without aggregate functions has no places to
/// record and no states to advance, so a hit costs nothing beyond the probe and a miss stages the key
/// alone - which is all a set has to carry.
template <typename LocalMethod, typename SharedMethod>
requires SetAggregationMethod<LocalMethod>
void NO_INLINE Aggregator::executeFrozenImpl(
    LocalMethod & local_method,
    std::type_identity<SharedMethod>,
    Arena *,
    const Columns & columns,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    AdaptiveAggregationProducer & adaptive,
    bool all_keys_are_const,
    MemoryTracker *) const
{
    using RecordKey = AdaptiveRecordKey<SharedMethod>;
    static_assert(SharedMethod::Data::NUM_BUCKETS == ADAPTIVE_AGGREGATION_NUM_BUCKETS);

    Arena scratch_pool;

    typename LocalMethod::StateNoCache local_find_state(key_columns, key_sizes, aggregation_state_cache);
    const auto keep_key_bytes = adaptiveMissesKeepKeyBytes<RecordKey>(local_find_state);
    /// The keys of the rows before `row_begin` (the part of the block aggregated before freezing) are not needed.
    /// If all keys are const, only row 0 is used.
    if constexpr (requires { local_find_state.skipRowsBefore(row_begin); })
    {
        if (!all_keys_are_const)
            local_find_state.skipRowsBefore(row_begin);
    }

    /// The kernel runs only while the producer is frozen, and phase transitions happen between
    /// blocks, so the reference stays valid for the whole block.
    auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    const bool bypass_local_probe = frozen.bypass_local_probe;

    auto & misses = adaptive.misses;
    const auto stage_misses = [&]
    {
        accountStagedBatch(
            columns, adaptive, appendDelayedRecords<RecordKey>(aggregate_instructions, adaptive, local_find_state, scratch_pool));
    };

    if (all_keys_are_const)
    {
        auto && key_holder = local_find_state.getKeyHolder(0, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);

        /// The whole range carries one key and a set stores a key once, so a single record stands
        /// for it however many rows it spans.
        const auto found = local_method.data.find(key, hash);
        if (!found)
        {
            misses.setKeysConst();
            misses.add<RecordKey>(static_cast<UInt32>(row_begin), hash, key, keep_key_bytes);
            stage_misses();
        }
        if (frozen.recordLocalProbeResults(found ? row_end - row_begin : 0, row_end - row_begin))
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);
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
            misses.add<RecordKey>(static_cast<UInt32>(i), hash, key, keep_key_bytes);

        keyHolderDiscardKey(key_holder);
        if constexpr (adaptive_misses_may_keep_key_bytes<RecordKey, typename LocalMethod::StateNoCache>)
        {
            if (missBatchKeyBytesFull(misses, keep_key_bytes))
                stage_misses();
        }
    }

    if (frozen.recordLocalProbeResults(hits, row_end - row_begin))
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);

    stage_misses();
}

template <typename RecordKey, typename LocalMethod, typename State, typename StageMisses>
size_t ALWAYS_INLINE Aggregator::collectCountMisses(
    LocalMethod & local_method,
    State & find_state,
    size_t row_begin,
    size_t row_end,
    Arena & scratch_pool,
    AdaptiveAggregationProducer & adaptive,
    bool bypass_local_probe,
    const StageMisses & stage_misses) const
{
    auto & misses = adaptive.misses;
    const auto keep_key_bytes = adaptiveMissesKeepKeyBytes<RecordKey>(find_state);
    constexpr bool can_combine_partition_counts = adaptive_key_stages_bytes<RecordKey>
        && requires { find_state.getKeyBytes(0); };
    [[maybe_unused]] const bool stable_key_views = find_state.keyViewsAreBlockStable();
    struct PartitionCountEntry
    {
        RecordKey key{};
        size_t record_index = std::numeric_limits<size_t>::max();
    };
    std::vector<PartitionCountEntry> last_partition_keys;
    const AdaptivePartitionLayout layout = adaptive.session->layout;
    if constexpr (can_combine_partition_counts)
    {
        chassert(stable_key_views);
        chassert(row_end - row_begin <= std::numeric_limits<UInt32>::max());
        last_partition_keys.resize(layout.numPartitions());
    }

    struct CountMissesResult
    {
        size_t next_row;
        size_t hits;
    };
    const auto collect_count_misses = [&, row_end, bypass_local_probe, layout]
        <bool combine_partition_counts>(size_t begin) ALWAYS_INLINE -> CountMissesResult
    {
        size_t hits = 0;
        /// Only this pass's new descriptors seed consecutive runs. The trial's last missed key may
        /// refer to an earlier descriptor rather than the last one in the array.
        const size_t first_record = can_combine_partition_counts ? misses.size() : 0;
        size_t last_record = 0;
        size_t combining_lookups = 0;
        size_t combined_records = 0;
        bool stop_combining = false;
        RecordKey last_staged_key{};
        for (size_t i = begin; i < row_end; ++i)
        {
            auto && key_holder = find_state.getKeyHolder(i, scratch_pool);
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

            const RecordKey staged_key = key;

            /// Without combining, the previous record is always the last descriptor. Spelling it so keeps
            /// `first_record` and `last_record` out of the loop of the keys that never combine.
            bool run_continues = (can_combine_partition_counts ? misses.size() != first_record : !misses.empty())
                && (combine_partition_counts ? misses.hashAt(last_record) : misses.lastHash()) == hash;
            if constexpr (std::is_same_v<RecordKey, std::string_view>)
                run_continues = run_continues && stable_key_views && staged_key == last_staged_key;
            else
                run_continues = run_continues && staged_key == last_staged_key;

            if (run_continues)
            {
                if constexpr (combine_partition_counts)
                    misses.incrementCount(last_record);
                else
                    misses.incrementLastCount();
            }
            else
            {
                size_t record = misses.size();
                if constexpr (combine_partition_counts)
                {
                    /// Keep each partition's last missed key for this block. Its bytes remain alive until
                    /// the append copies them into owned records; only the descriptor's count changes here.
                    /// Packed-key equality checks its inline signature before comparing the string bytes.
                    auto & last = last_partition_keys[layout.partitionOf(hash)];
                    if (last.record_index != std::numeric_limits<size_t>::max()
                        && (std::is_same_v<RecordKey, PackedStringRef> || misses.hashAt(last.record_index) == hash)
                        && staged_key == last.key)
                    {
                        record = last.record_index;
                        ++combined_records;
                    }
                    else
                    {
                        last = {staged_key, record};
                    }
                    ++combining_lookups;
                    if (combining_lookups == adaptive_count_combining_sample_records
                        && combined_records * adaptive_count_combining_min_hit_rate_inverse < combining_lookups)
                    {
                        stop_combining = true;
                    }
                }

                if (record == misses.size())
                    misses.addCount<RecordKey>(static_cast<UInt32>(i), hash, staged_key, 1, keep_key_bytes);
                else
                    misses.incrementCount(record);
                last_record = record;

                /// A serialized key view points into the reused scratch arena and can only seed
                /// the run tracking when the views are block-stable; every other key type is
                /// either a self-contained value or, for a packed reference, points into the
                /// block's key column, whose bytes remain alive throughout the row loop.
                if constexpr (std::is_same_v<RecordKey, std::string_view>)
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
            if constexpr (combine_partition_counts)
            {
                if (stop_combining)
                    return {i + 1, hits};
            }
            if constexpr (adaptive_misses_may_keep_key_bytes<RecordKey, State>)
            {
                /// The key views of a state whose batch keeps the key bytes do not outlive their row, so no record
                /// takes more rows, and staging the batch in the middle of the block splits none. Combining needs the
                /// key bytes of `getKeyBytes`, which such a state lacks.
                static_assert(!combine_partition_counts);
                if (missBatchKeyBytesFull(misses, keep_key_bytes))
                {
                    chassert(!stable_key_views);
                    stage_misses();
                }
            }
        }
        return {row_end, hits};
    };

    size_t hits = 0;
    if constexpr (can_combine_partition_counts)
    {
        const auto trial = collect_count_misses.template operator()<true>(row_begin);
        hits = trial.hits;
        if (trial.next_row != row_end)
        {
            /// Keep the trial's descriptors for the common append. The remainder tracks consecutive
            /// runs among its own new descriptors and performs no partition-cache lookups.
            hits += collect_count_misses.template operator()<false>(trial.next_row).hits;
        }
    }
    else
    {
        hits = collect_count_misses.template operator()<false>(row_begin).hits;
    }
    return hits;
}

template <typename LocalMethod, typename SharedMethod>
requires MapAggregationMethod<LocalMethod>
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
    bool all_keys_are_const,
    MemoryTracker * local_memory_tracker) const
{
    using RecordKey = AdaptiveRecordKey<SharedMethod>;
    static_assert(SharedMethod::Data::NUM_BUCKETS == ADAPTIVE_AGGREGATION_NUM_BUCKETS);

    Arena scratch_pool;

    typename LocalMethod::StateNoCache local_find_state(key_columns, key_sizes, aggregation_state_cache);
    const auto keep_key_bytes = adaptiveMissesKeepKeyBytes<RecordKey>(local_find_state);
    /// Routing needs only the two-level twin's TYPE: the hash is the local table's canonical
    /// hash (identical to the twin's by construction - the pairing in `executeFrozen` binds a
    /// method to its own two-level form, which keeps the hash function), and the partitioning
    /// of the hash is static.

    /// The keys of the rows before `row_begin` (the part of the block aggregated before freezing) are not needed.
    /// If all keys are const, only row 0 is used.
    if constexpr (requires { local_find_state.skipRowsBefore(row_begin); })
    {
        if (!all_keys_are_const)
            local_find_state.skipRowsBefore(row_begin);
    }

    /// The kernel runs only while the producer is frozen, and phase transitions happen between
    /// blocks, so the reference stays valid for the whole block.
    auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    const auto record_local_probes = [&](size_t hits, size_t rows)
    {
        if (frozen.recordLocalProbeResults(hits, rows))
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);
    };
    const bool bypass_local_probe = frozen.bypass_local_probe;
    auto & misses = adaptive.misses;
    const auto stage_misses = [&]
    {
        accountStagedBatch(
            columns, adaptive, appendDelayedRecords<RecordKey>(aggregate_instructions, adaptive, local_find_state, scratch_pool));
    };

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
                /// Local state growth belongs to this table, while the staging runs under the aggregator's tracker.
                std::optional<MemoryTrackerSwitcher> local_scope;
                if (local_memory_tracker)
                    local_scope.emplace(local_memory_tracker);

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
            misses.setKeysConst();
            if (is_simple_count)
            {
                misses.addCount<RecordKey>(
                    static_cast<UInt32>(row_begin), hash, key, static_cast<UInt32>(row_end - row_begin), keep_key_bytes);
            }
            else
            {
                for (size_t i = row_begin; i < row_end; ++i)
                    misses.add<RecordKey>(static_cast<UInt32>(i), hash, key, keep_key_bytes);
            }
            stage_misses();
        }
        record_local_probes(found ? row_end - row_begin : 0, row_end - row_begin);
        keyHolderDiscardKey(key_holder);
        return;
    }

    if (is_simple_count)
    {
        record_local_probes(
            collectCountMisses<RecordKey>(
                local_method, local_find_state, row_begin, row_end, scratch_pool, adaptive, bypass_local_probe, stage_misses),
            row_end - row_begin);
        stage_misses();
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
            misses.add<RecordKey>(static_cast<UInt32>(i), hash, key, keep_key_bytes);
            keyHolderDiscardKey(key_holder);
            if constexpr (adaptive_misses_may_keep_key_bytes<RecordKey, typename LocalMethod::StateNoCache>)
            {
                if (missBatchKeyBytesFull(misses, keep_key_bytes))
                    stage_misses();
            }
        }
        return hits;
    };

    /// Without aggregate functions there is nothing to record for a hit; with the probe bypassed there is no hit.
    if (params.aggregates_size == 0 || bypass_local_probe)
    {
        const size_t hits = probe_rows.template operator()<false>(nullptr);
        record_local_probes(hits, row_end - row_begin);
        stage_misses();
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
    record_local_probes(hits, row_end - row_begin);
    stage_misses();

    /// With no local hits every place is null and the batch pass would only skip rows; the
    /// staged records carry the block's whole contribution.
    if (hits != 0)
    {
        /// Keep the scratch allocations and staged records outside the local table's estimate.
        std::optional<MemoryTrackerSwitcher> local_scope;
        if (local_memory_tracker)
            local_scope.emplace(local_memory_tracker);

        executeAggregateInstructions(
            aggregates_pool,
            row_begin,
            row_end,
            aggregate_instructions,
            places.get(),
            /*key_start=*/row_begin,
            /*has_only_one_value_since_last_reset=*/false,
            /*all_keys_are_const=*/false,
            /*all_places_are_non_null=*/false,
            /*use_compiled_functions=*/false);
    }
}

template <typename RecordKey, typename State>
AdaptiveStagedBytes NO_INLINE Aggregator::appendDelayedRecords(
    const AggregateFunctionInstruction * aggregate_instructions,
    AdaptiveAggregationProducer & adaptive,
    State & local_find_state,
    Arena & scratch_pool) const
{
    const auto & misses = adaptive.misses;
    const size_t total = misses.size();
    if (!total)
        return {};

    auto & partitions = *adaptive.partitions;
    const AdaptivePartitionLayout layout = adaptive.session->layout;
    const auto * hashes = misses.getHashes().data();
    const auto * source_rows = misses.getSourceRows().data();
    const auto * staged_key_sizes = misses.getKeySizes().data();
    const char * staged_keys = misses.getKeys().data();
    const bool keys_are_const = misses.keysAreConst();

    /// A constant key is read from the key columns' single row, while the arguments of each record stay its own
    /// source row.
    const auto key_row_of = [source_rows, keys_are_const](size_t record) -> size_t
    {
        return keys_are_const ? 0 : source_rows[record];
    };
    const auto key_size_of = [&](size_t record) -> size_t
    {
        if constexpr (adaptive_key_stages_bytes<RecordKey>)
            return staged_key_sizes[record];
        else
            return sizeof(RecordKey);
    };

    /// The appends go to random partitions, so the loops below prefetch the append position of
    /// the record `adaptive_append_prefetch_distance` ahead.
    const auto prefetch_append = [&partitions, layout, hashes, total](size_t record) ALWAYS_INLINE
    {
        if (record + adaptive_append_prefetch_distance < total)
            partitions.prefetchAppend(layout.partitionOf(hashes[record + adaptive_append_prefetch_distance]));
    };

    /// The records are written in staging order, so they take the key bytes that the batch kept in their order, and the
    /// records of a constant key all take its one copy.
    const auto batch_keeps_key_bytes = adaptiveMissesKeepKeyBytes<RecordKey>(local_find_state);
    const char * kept_key_bytes = staged_keys;
    const auto write_key = [&](size_t record, size_t key_size, char * to)
    {
        if constexpr (adaptive_key_stages_bytes<RecordKey>)
        {
            if (batch_keeps_key_bytes)
            {
                copyPaddedKeyBytes(kept_key_bytes, key_size, to);
                if (!keys_are_const)
                    kept_key_bytes += key_size;
            }
            else
                copyStagedKeyBytes(local_find_state, key_row_of(record), key_size, scratch_pool, to);
        }
        else
            memcpy(to, staged_keys + record * sizeof(RecordKey), sizeof(RecordKey));
    };

    size_t key_bytes = 0;
    size_t record_bytes = 0;

    const auto append_key_records = [&]<typename Record>(Record) ALWAYS_INLINE
    {
        for (size_t i = 0; i < total; ++i)
        {
            prefetch_append(i);
            const UInt64 hash = hashes[i];
            const size_t partition = layout.partitionOf(hash);
            const size_t key_size = key_size_of(i);
            const size_t bytes = Record::bytesForKey(key_size);
            char * record = partitions.append(partition, bytes);
            Record::writeHeader(record, hash, Record::with_count ? misses.getMultiplicities()[i] : 0, key_size);
            write_key(i, key_size, record + Record::key_offset);
            zeroStagedRecordPadding(record + Record::key_offset + key_size);
            key_bytes += key_size;
            record_bytes += bytes;
        }
    };

    const auto append_argument_records = [&]<typename Record>(Record format) ALWAYS_INLINE
    {
        const auto & argument_layout = *adaptive_argument_layout;

        /// The arguments are staged in the form the drain rebuilds and the instructions consume (see
        /// `AdaptiveArgumentLayout::build`): the instructions' `arguments`, which `prepareAggregateInstructions` built
        /// for the whole block with constant, replicated and `LowCardinality` columns materialized. They hold whole
        /// rows, unlike the `batch_arguments` that an `-Array` combinator unnests: the drain rebuilds the instructions
        /// from the staged rows and unnests them again. Building them again from the block's own columns would copy
        /// each such column once more, however few rows miss. Of a column that several aggregates read, a dense form
        /// that one of them prepared is preferred to a sparse one, which takes a pass over its offsets (below).
        std::vector<ArgumentSource> arguments(argument_layout.num_positions, {nullptr, source_rows});
        for (size_t i = 0; i < params.aggregates_size; ++i)
            for (size_t j = 0; j < aggregates_positions[i].size(); ++j)
            {
                const IColumn *& argument = arguments[aggregates_positions[i][j]].column;
                if (!argument || argument->isSparse())
                    argument = aggregate_instructions[i].arguments[j];
            }

        /// A sparse column, which an aggregate of one argument consumes as it is, is read through its values: the
        /// source rows of the records ascend, so one pass of an iterator along its offsets finds the value of every
        /// record. A dense copy of the column would also copy the values of the rows that hit the local table.
        std::vector<PaddedPODArray<UInt32>> value_rows(argument_layout.num_positions);
        const auto read_through_values = [&](size_t position)
        {
            ArgumentSource & argument = arguments[position];
            const auto * sparse = typeid_cast<const ColumnSparse *>(argument.column);
            if (!sparse)
                return;
            auto & rows = value_rows[position];
            rows.resize(total);
            auto iterator = sparse->getIterator(source_rows[0]);
            for (size_t i = 0; i < total; ++i)
            {
                iterator.advanceToRow(source_rows[i]);
                rows[i] = static_cast<UInt32>(iterator.getValueIndex());
            }
            argument = {&sparse->getValuesColumn(), rows.data()};
        };

        for (const auto & field : argument_layout.fixed_fields)
            read_through_values(field.position);

        std::vector<ArgumentSource> variable_sources;
        variable_sources.reserve(argument_layout.variable_fields.size());
        for (const auto & field : argument_layout.variable_fields)
        {
            read_through_values(field.position);
            variable_sources.push_back(arguments[field.position]);
        }

        /// A value whose serialized size cannot be computed in advance is serialized into a scratch arena first and
        /// copied from there. The values of one row form one contiguous range, which is rolled back once the record is
        /// written, so the scratch copies do not accumulate until the spill check. The arena is separate from
        /// `scratch_pool`, where the key holders of `write_key` may allocate and roll back in between.
        const IColumn::SerializationSettings serialization_settings;
        std::vector<std::optional<size_t>> serialized_in_scratch(variable_sources.size());
        std::optional<Arena> argument_scratch;

        /// A `String` argument, the common variable-size case, is staged by `writeStagedString` (see `isStagedString`). The
        /// other variable-size arguments take their column's own serialization.
        std::vector<const ColumnString *> string_sources(variable_sources.size(), nullptr);
        for (size_t j = 0; j < variable_sources.size(); ++j)
            if (isStagedString(argument_layout.variable_fields[j]))
                string_sources[j] = &assert_cast<const ColumnString &>(*variable_sources[j].column);

        std::array<char *, adaptive_argument_staging_batch_rows> argument_rows{};
        for (size_t batch_begin = 0; batch_begin < total; batch_begin += argument_rows.size())
        {
            const size_t batch_end = std::min(total, batch_begin + argument_rows.size());
            if constexpr (std::is_same_v<Record, StagedFixedArgumentRecord<RecordKey>>)
            {
                for (size_t i = batch_begin; i < batch_end; ++i)
                {
                    prefetch_append(i);
                    const size_t partition = layout.partitionOf(hashes[i]);
                    char * record = partitions.append(partition, format.stride);
                    write_key(i, sizeof(RecordKey), record + Record::key_offset);
                    argument_rows[i - batch_begin] = record + Record::fixed_arguments_offset;
                }
                key_bytes += (batch_end - batch_begin) * sizeof(RecordKey);
                record_bytes += (batch_end - batch_begin) * format.stride;
            }
            else if constexpr (std::is_same_v<Record, StagedStringArgumentRecord<RecordKey>>)
            {
                /// The format is chosen only when every variable-size argument is staged as a `String`.
                chassert(argument_layout.variable_fields_are_strings);
                for (size_t i = batch_begin; i < batch_end; ++i)
                {
                    prefetch_append(i);
                    const size_t partition = layout.partitionOf(hashes[i]);
                    size_t variable_bytes = 0;
                    for (size_t j = 0; j < string_sources.size(); ++j)
                        variable_bytes += stagedStringBytes(string_sources[j]->getDataAt(variable_sources[j].rows[i]));

                    const size_t bytes = format.key_offset + sizeof(RecordKey) + variable_bytes;
                    char * record = partitions.append(partition, bytes);
                    argument_rows[i - batch_begin] = record + Record::fixed_arguments_offset;
                    write_key(i, sizeof(RecordKey), record + format.key_offset);
                    char * variable = record + format.key_offset + sizeof(RecordKey);
                    for (size_t j = 0; j < string_sources.size(); ++j)
                        variable = writeStagedString(variable, string_sources[j]->getDataAt(variable_sources[j].rows[i]));
                    record_bytes += bytes;
                }
                key_bytes += (batch_end - batch_begin) * sizeof(RecordKey);
            }
            else
            {
                for (size_t i = batch_begin; i < batch_end; ++i)
                {
                    prefetch_append(i);
                    const UInt64 hash = hashes[i];
                    const size_t partition = layout.partitionOf(hash);
                    const size_t key_size = key_size_of(i);

                    size_t variable_bytes = 0;
                    const char * scratch_begin = nullptr;
                    size_t scratch_bytes = 0;
                    for (size_t j = 0; j < variable_sources.size(); ++j)
                    {
                        const auto & [column, rows] = variable_sources[j];
                        if (const auto * string = string_sources[j])
                        {
                            serialized_in_scratch[j].reset();
                            variable_bytes += stagedStringBytes(string->getDataAt(rows[i]));
                        }
                        else if (const auto size = column->getSerializedValueSize(rows[i], &serialization_settings))
                        {
                            serialized_in_scratch[j].reset();
                            variable_bytes += *size;
                        }
                        else
                        {
                            /// Continuing the range may move it, so only the sizes are kept, and the values are read
                            /// from `scratch_begin` afterwards.
                            if (!argument_scratch)
                                argument_scratch.emplace();
                            const size_t size_of_value
                                = column->serializeValueIntoArena(rows[i], *argument_scratch, scratch_begin, &serialization_settings).size();
                            serialized_in_scratch[j] = size_of_value;
                            variable_bytes += size_of_value;
                            scratch_bytes += size_of_value;
                        }
                    }

                    const size_t bytes = alignStagedRecord(format.key_offset + key_size + variable_bytes);
                    char * record = partitions.append(partition, bytes);
                    Record::writeHeader(record, hash, bytes, key_size);
                    argument_rows[i - batch_begin] = record + Record::fixed_arguments_offset;

                    /// The key goes before the variable-size arguments, which overwrite whatever its copy
                    /// scribbled past it.
                    char * key = record + format.key_offset;
                    write_key(i, key_size, key);

                    char * variable = key + key_size;
                    const char * scratch = scratch_begin;
                    for (size_t j = 0; j < variable_sources.size(); ++j)
                    {
                        const auto & [column, rows] = variable_sources[j];
                        if (const auto * string = string_sources[j])
                            variable = writeStagedString(variable, string->getDataAt(rows[i]));
                        else if (const auto & serialized_size = serialized_in_scratch[j])
                        {
                            memcpy(variable, scratch, *serialized_size);
                            variable += *serialized_size;
                            scratch += *serialized_size;
                        }
                        else
                            variable = column->serializeValueIntoMemory(rows[i], variable, &serialization_settings);
                    }
                    zeroStagedRecordPadding(variable);
                    if (scratch_bytes)
                        argument_scratch->rollback(scratch_bytes);

                    key_bytes += key_size;
                    record_bytes += bytes;
                }
            }

            const size_t batch_rows = batch_end - batch_begin;
            for (const auto & field : argument_layout.fixed_fields)
            {
                const auto & [column, rows] = arguments[field.position];
                RowDataStore::gatherFieldToRows(
                    *column,
                    {rows + batch_begin, batch_rows},
                    {argument_rows.data(), batch_rows},
                    field.offset);
            }
        }
    };

    visitStagedRecordFormat<RecordKey>(adaptive_argument_layout.get(), is_simple_count, [&]<typename Record>(Record format) ALWAYS_INLINE
    {
        if constexpr (Record::with_arguments)
            append_argument_records(format);
        else
            append_key_records(format);
    });
    /// The kernel and the writer decide on keeping the key bytes from the same state, and every kept byte belongs to
    /// exactly one record, or to all the records of a constant key.
    chassert(!batch_keeps_key_bytes
        || misses.getKeys().size() == (keys_are_const ? key_size_of(0) : static_cast<size_t>(kept_key_bytes - staged_keys)));

    return {.key_bytes = key_bytes, .record_bytes = record_bytes};
}

}
