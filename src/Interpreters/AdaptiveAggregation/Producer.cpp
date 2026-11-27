/// The producer kernels of the adaptive aggregation: the frozen consume path, the staging of missed rows as
/// partition records, and the scan of a frozen table for the local top of the top-K pruning. They are member
/// templates of `Aggregator` (defined here rather than in `Aggregator.cpp`, following
/// `ClientBaseOptimizedParts.cpp`), dispatched over the aggregation-method variants.

#include <algorithm>
#include <functional>
#include <limits>
#include <optional>
#include <type_traits>
#include <vector>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/findExtreme.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregationImpl.h>
#include <Interpreters/AdaptiveAggregation/Combining/AdaptiveMissCombiner.h>
#include <Interpreters/AdaptiveAggregation/Combining/AdaptiveRowSelection.h>
#include <Interpreters/Aggregator.h>
#include <base/sort.h>

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
    /// Whether `State` reads the bytes of its keys in place, from the block's padded column buffers, which
    /// hold them for the whole block: the bytes of a string key by `getKeyBytes`, and the packed values and
    /// the string of a key of fixed-width values and a string by `getKeyParts`.
    template <typename State>
    constexpr bool adaptive_state_reads_key_bytes_in_place = requires(const State & state) { state.getKeyBytes(size_t{}); }
        || requires(const State & state) { state.getKeyParts(size_t{}); };

    /// Whether the kernel of `State` may keep the bytes of its missed string-like keys in the batch (see
    /// `AdaptiveMissBatch`): a state that reads them in place never keeps them. The probe loop checks the
    /// bound on the kept bytes only under this (see `FrozenTableProbe`): the check may stage the batch, a
    /// call that the loops of the other states would carry for nothing.
    template <typename RecordKey, typename State>
    constexpr bool adaptive_misses_may_keep_key_bytes
        = adaptive_key_stages_bytes<RecordKey> && !adaptive_state_reads_key_bytes_in_place<State>;

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

    /// What the probe loop of a kernel whose records carry no aggregate arguments does with its rows besides
    /// staging the misses (see `FrozenTableProbe`). A hit of an aggregation by a lone `count` (`with_count`)
    /// adds one to the key's inline count, and a hit of one without aggregate functions costs nothing beyond
    /// the probe. A row of the NULL key is only counted: the caller adds the rows to the key's slot.
    template <bool with_count>
    struct KeyRecordRows
    {
        static constexpr bool checks_null_rows = true;

        /// The lookup result comes by value: that of a string table reaches the mapped value only when not
        /// const.
        template <typename LookupResult>
        void ALWAYS_INLINE hit(size_t, LookupResult it) const
        {
            if constexpr (with_count)
                ++getInlineCountState(it->getMapped());
        }
        void ALWAYS_INLINE miss(size_t) const { }
        template <typename Data>
        void ALWAYS_INLINE nullRow(size_t, Data &) const { }
    };

    /// What the probe loop of a kernel whose records carry aggregate arguments does with its rows besides
    /// staging the misses (see `FrozenTableProbe`). With `record_places` every row gets its place in
    /// `places`: the key's for a hit, null for a miss, and the slot of the NULL key (`emplace_null_key`) for
    /// a row of that key. Without it the kernel bypasses the probe of a block that has no rows of the NULL
    /// key, so no row needs a place.
    template <bool record_places, typename EmplaceNullKey>
    struct ArgumentRecordRows
    {
        static constexpr bool checks_null_rows = record_places;

        template <typename LookupResult>
        void ALWAYS_INLINE hit(size_t row, LookupResult it) const
        {
            if constexpr (record_places)
                places[row] = it->getMapped();
        }
        void ALWAYS_INLINE miss(size_t row) const
        {
            if constexpr (record_places)
                places[row] = nullptr;
        }
        template <typename Data>
        void ALWAYS_INLINE nullRow(size_t row, Data & data) const
        {
            if constexpr (record_places)
                places[row] = emplace_null_key(data);
        }

        AggregateDataPtr * places;
        const EmplaceNullKey & emplace_null_key;
    };

    /// The probe loop of a frozen kernel over rows of its block (see `combineRowMisses`). A row whose key the
    /// local table holds is a hit, unless the probe is bypassed, and any other row is a miss, which goes to
    /// the stager that the driver passes. `Rows` tells what else the kernel does with a row (see
    /// `KeyRecordRows` and `ArgumentRecordRows`), including a row of the NULL key that the method keeps in a
    /// slot of its own, which takes no lookup (see `AdaptiveAggregationProducer::FrozenState`). A batch whose
    /// kept key bytes reach their bound is staged by `stage_misses` in the middle of the block (see
    /// `adaptive_miss_batch_max_key_bytes`). The caller guarantees that everything the probe refers to
    /// outlives it.
    template <typename RecordKey, typename LocalMethod, typename State, typename KeepKeyBytes, typename StageMisses, typename Rows>
    class FrozenTableProbe
    {
    public:
        static constexpr size_t chunk_rows = AdaptiveTailsGate::chunk_rows;
        static constexpr bool reports_lookups = true;

        FrozenTableProbe(
            LocalMethod & local_method_,
            State & find_state_,
            Arena & scratch_pool_,
            AdaptiveMissBatch & misses_,
            bool bypass_local_probe_,
            KeepKeyBytes keep_key_bytes_,
            const StageMisses & stage_misses_,
            Rows rows_)
            : local_method(local_method_)
            , find_state(find_state_)
            , scratch_pool(scratch_pool_)
            , misses(misses_)
            , bypass_local_probe(bypass_local_probe_)
            , keep_key_bytes(keep_key_bytes_)
            , stage_misses(stage_misses_)
            , rows(rows_)
        {
        }

        /// Probes the rows from `begin` to `end`, stages their misses through `stager`, and returns the hits
        /// and the rows of the NULL key.
        template <typename Stager>
        AdaptiveLocalProbes ALWAYS_INLINE probeRows(Stager & stager, size_t begin, size_t end) const
        {
            AdaptiveLocalProbes probes;
            /// The last missed key of fixed-width values and a string, and its hash, for the rows that repeat
            /// it. The loop keeps it for the rows of one call, so the first such key of a call is hashed and
            /// probed.
            [[maybe_unused]] RecordKey last_missed_key{};
            [[maybe_unused]] UInt64 last_missed_hash = 0;
            [[maybe_unused]] bool has_missed_key = false;
            for (size_t i = begin; i < end; ++i)
            {
                if constexpr (keeps_null_key_slot<LocalMethod> && Rows::checks_null_rows)
                {
                    if (find_state.isNullAt(i))
                    {
                        ++probes.null_rows;
                        rows.nullRow(i, local_method.data);
                        continue;
                    }
                }

                auto && key_holder = find_state.getKeyHolder(i, scratch_pool);
                const auto & key = keyHolderGetKey(key_holder);

                if constexpr (is_keys_and_packed_string<RecordKey>)
                {
                    /// A key with an empty string that repeats the last missed key, as the rows of a missing
                    /// value often do, misses as well: it is missing from the local table. It goes to the
                    /// stager with that key's hash without being hashed or probed again, and its fixed-width
                    /// values decide it, as an empty string carries no hash.
                    if (key.str.isEmpty() && has_missed_key && last_missed_key.str.isEmpty() && key.keys == last_missed_key.keys)
                    {
                        rows.miss(i);
                        stager.add(static_cast<UInt32>(i), last_missed_hash, key);
                        keyHolderDiscardKey(key_holder);
                        continue;
                    }
                }

                const UInt64 hash = local_method.data.hash(key);

                if (!bypass_local_probe)
                {
                    if (auto it = local_method.data.find(key, hash))
                    {
                        ++probes.hits;
                        rows.hit(i, it);
                        keyHolderDiscardKey(key_holder);
                        continue;
                    }
                }

                rows.miss(i);
                stager.add(static_cast<UInt32>(i), hash, key);
                if constexpr (is_keys_and_packed_string<RecordKey>)
                {
                    last_missed_key = key;
                    last_missed_hash = hash;
                    has_missed_key = true;
                }
                keyHolderDiscardKey(key_holder);
                if constexpr (adaptive_misses_may_keep_key_bytes<RecordKey, State>)
                {
                    if (missBatchKeyBytesFull(misses, keep_key_bytes))
                        stage_misses();
                }
            }
            return probes;
        }

    private:
        LocalMethod & local_method;
        State & find_state;
        Arena & scratch_pool;
        AdaptiveMissBatch & misses;
        const bool bypass_local_probe;
        const KeepKeyBytes keep_key_bytes;
        const StageMisses & stage_misses;
        const Rows rows;
    };

    /// The probe of a block whose keys are all one constant key, of `hash`, that missed the frozen table (see
    /// `combineRowMisses`). It takes no lookup per row, so the driver hands it the whole block at once, as
    /// one chunk. Every row repeats the key, so the misses of the block are misses of the key that each stand
    /// for a range of its rows: one for all of them, or, for a row selection that counts rows, one per range
    /// of `rows_per_miss` rows, because a record stands for at most `staged_multiplicity_max` rows. The
    /// misses after the first repeat it, so they combine into its record while the record takes their rows.
    /// The miss of a row selection takes the arguments of the first row of the largest rank of its range, so
    /// the block's best row is among the records.
    template <typename RecordKey, AdaptiveMissCombine kind>
    class ConstantKeyMissProbe
    {
    public:
        static constexpr size_t chunk_rows = std::numeric_limits<size_t>::max();
        /// The block looks the tail up once, whatever its rows, so it has no say in whether the lookups pay.
        /// Counted by its rows (see `AdaptiveMissCombiner::takeChunkLookups`), a record that it starts would
        /// weigh as many lookups without a hit.
        static constexpr bool reports_lookups = false;

        /// `ranks` are the producer's ranks of the block, which a row selection requires and the other kinds
        /// ignore.
        ConstantKeyMissProbe(UInt64 hash_, const RecordKey & key_, AdaptiveRowSelectionRanks * ranks_, size_t rows_per_miss_)
            : hash(hash_)
            , key(key_)
            , ranks(ranks_)
            , rows_per_miss(rows_per_miss_)
        {
        }

        template <typename Stager>
        AdaptiveLocalProbes ALWAYS_INLINE probeRows(Stager & stager, size_t begin, size_t end) const
        {
            if constexpr (kind == AdaptiveMissCombine::selection)
            {
                ranks->rankUpTo(end);
                const Int64 * row_ranks = ranks->data();
                for (size_t range_begin = begin; range_begin < end;)
                {
                    const size_t range_end = range_begin + std::min(end - range_begin, rows_per_miss);
                    const size_t best = *findExtremeMaxIndex(row_ranks, range_begin, range_end);
                    stager.add(static_cast<UInt32>(best), hash, key, static_cast<UInt32>(range_end - range_begin));
                    range_begin = range_end;
                }
            }
            else
                stager.add(static_cast<UInt32>(begin), hash, key, static_cast<UInt32>(end - begin));
            return {};
        }

    private:
        const UInt64 hash;
        const RecordKey & key;
        AdaptiveRowSelectionRanks * const ranks;
        const size_t rows_per_miss;
    };

    /// Stages the misses of the rows from `row_begin` to `row_end` of a block whose keys are all one constant
    /// key, of `hash`, that missed the frozen table, through the tails of the producer's partitions (see
    /// `ConstantKeyMissProbe`). `selection` is the aggregation's row selection, which a row selection
    /// requires.
    template <typename RecordKey, AdaptiveMissCombine kind, typename KeepKeyBytes>
    void ALWAYS_INLINE stageConstantKeyMisses(
        AdaptiveAggregationProducer & adaptive,
        KeepKeyBytes keep_key_bytes,
        const AdaptiveRowSelection * selection,
        UInt64 hash,
        const RecordKey & key,
        size_t row_begin,
        size_t row_end)
    {
        const bool counts_rows = kind == AdaptiveMissCombine::selection && selection->countsRows();
        const ConstantKeyMissProbe<RecordKey, kind> probe(
            hash,
            key,
            kind == AdaptiveMissCombine::selection ? &adaptive.row_selection_ranks : nullptr,
            counts_rows ? size_t{staged_multiplicity_max} : row_end - row_begin);
        combineRowMisses<RecordKey, kind, KeepKeyBytes>(adaptive, keep_key_bytes, selection, row_begin, row_end, probe);
    }

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

std::optional<UInt64> Aggregator::adaptiveLocalTopKCount(AggregatedDataVariants & table, const AdaptiveTopKCountBins & bins) const
{
    chassert(is_simple_count);
    std::vector<UInt64> counts;
    const auto append_table_counts = [&](auto & method)
    {
        using Method = std::decay_t<decltype(method)>;
        if constexpr (MapAggregationMethod<Method>)
        {
            method.data.forEachMapped([&](AggregateDataPtr & mapped) { counts.push_back(getInlineCountState(mapped)); });
            /// The NULL key is a group of the table outside its cells (see `keeps_null_key_slot`).
            if constexpr (keeps_null_key_slot<Method>)
                if (method.data.hasNullKeyData())
                    counts.push_back(getInlineCountState(method.data.getNullKeyData()));
        }
    };
#define M(NAME) \
    else if (table.type == AggregatedDataVariants::Type::NAME) \
        append_table_counts(*table.NAME);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
    else
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant in the adaptive frozen path.");

    bins.appendNonEmpty(counts);
    if (counts.size() < params.bucket_top_k)
        return std::nullopt;
    ::nth_element(counts.begin(), counts.begin() + (params.bucket_top_k - 1), counts.end(), std::greater<>());
    return counts[params.bucket_top_k - 1];
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
            columns, adaptive, appendDelayedRecords<SharedMethod>(aggregate_instructions, adaptive, local_find_state, scratch_pool));
    };

    if (all_keys_are_const)
    {
        /// A NULL key goes to its slot without a lookup (see `AdaptiveAggregationProducer::FrozenState`).
        if constexpr (keeps_null_key_slot<LocalMethod>)
        {
            if (local_find_state.isNullAt(0))
            {
                local_method.data.hasNullKeyData() = true;
                return;
            }
        }

        auto && key_holder = local_find_state.getKeyHolder(0, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);

        /// The whole range carries one key and a set stores a key once, so at most a single record stands for
        /// it however many rows it spans: none when the key is that of its partition's tail.
        const auto found = local_method.data.find(key, hash);
        if (!found)
        {
            misses.setKeysConst();
            using KeepKeyBytes = std::remove_cv_t<decltype(keep_key_bytes)>;
            stageConstantKeyMisses<RecordKey, AdaptiveMissCombine::keys, KeepKeyBytes>(
                adaptive, keep_key_bytes, /*selection=*/nullptr, hash, key, row_begin, row_end);
            stage_misses();
        }
        if (frozen.recordLocalProbeResults(found ? row_end - row_begin : 0, row_end - row_begin))
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);
        keyHolderDiscardKey(key_holder);
        return;
    }

    /// A set stores a key once, so a miss of the key of its partition's tail combines into the tail (see
    /// `AdaptivePartitionTails`).
    const AdaptiveLocalProbes probes = collectCombinedMisses</*with_count=*/false, RecordKey>(
        local_method, local_find_state, row_begin, row_end, scratch_pool, adaptive, bypass_local_probe, stage_misses);

    if constexpr (keeps_null_key_slot<LocalMethod>)
    {
        if (probes.null_rows)
            local_method.data.hasNullKeyData() = true;
    }
    if (frozen.recordLocalProbeResults(probes.hits, row_end - row_begin - probes.null_rows))
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);

    stage_misses();
}

template <bool with_count, typename RecordKey, typename LocalMethod, typename State, typename StageMisses>
AdaptiveLocalProbes ALWAYS_INLINE Aggregator::collectCombinedMisses(
    LocalMethod & local_method,
    State & find_state,
    size_t row_begin,
    size_t row_end,
    Arena & scratch_pool,
    AdaptiveAggregationProducer & adaptive,
    bool bypass_local_probe,
    const StageMisses & stage_misses) const
{
    const auto keep_key_bytes = adaptiveMissesKeepKeyBytes<RecordKey>(find_state);
    using KeepKeyBytes = std::remove_cv_t<decltype(keep_key_bytes)>;
    using Rows = KeyRecordRows<with_count>;
    const FrozenTableProbe<RecordKey, LocalMethod, State, KeepKeyBytes, StageMisses, Rows> probe(
        local_method, find_state, scratch_pool, adaptive.misses, bypass_local_probe, keep_key_bytes, stage_misses, Rows{});
    constexpr auto kind = with_count ? AdaptiveMissCombine::counts : AdaptiveMissCombine::keys;
    return combineRowMisses<RecordKey, kind, KeepKeyBytes>(adaptive, keep_key_bytes, /*selection=*/nullptr, row_begin, row_end, probe);
}

template <bool record_places, typename RecordKey, typename LocalMethod, typename State, typename EmplaceNullKey, typename StageMisses>
AdaptiveLocalProbes ALWAYS_INLINE Aggregator::collectArgumentMisses(
    LocalMethod & local_method,
    State & local_find_state,
    size_t row_begin,
    size_t row_end,
    Arena & scratch_pool,
    AdaptiveAggregationProducer & adaptive,
    bool bypass_local_probe,
    AggregateDataPtr * places_data,
    const EmplaceNullKey & emplace_null_key,
    const StageMisses & stage_misses) const
{
    const auto keep_key_bytes = adaptiveMissesKeepKeyBytes<RecordKey>(local_find_state);
    using KeepKeyBytes = std::remove_cv_t<decltype(keep_key_bytes)>;
    using Rows = ArgumentRecordRows<record_places, EmplaceNullKey>;
    const FrozenTableProbe<RecordKey, LocalMethod, State, KeepKeyBytes, StageMisses, Rows> probe(
        local_method,
        local_find_state,
        scratch_pool,
        adaptive.misses,
        bypass_local_probe,
        keep_key_bytes,
        stage_misses,
        Rows{.places = places_data, .emplace_null_key = emplace_null_key});

    /// Under a row selection the misses of a key combine into the tail of its partition, whose record keeps
    /// the best of their rows (see `AdaptiveMissCombiner`). Otherwise every miss is a record of its own.
    if (adaptive_row_selection)
        return combineRowMisses<RecordKey, AdaptiveMissCombine::selection, KeepKeyBytes>(
            adaptive, keep_key_bytes, adaptive_row_selection.get(), row_begin, row_end, probe);
    AdaptiveMissStager<RecordKey, KeepKeyBytes> stager{adaptive.misses, keep_key_bytes};
    return probe.probeRows(stager, row_begin, row_end);
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
            columns, adaptive, appendDelayedRecords<SharedMethod>(aggregate_instructions, adaptive, local_find_state, scratch_pool));
    };

    /// Returns the slot of the NULL key, which takes the rows of the key without a lookup (see
    /// `AdaptiveAggregationProducer::FrozenState`). The first of them fills the slot as the ordinary
    /// aggregation fills a new cell: with created states (see `executeImplBatch`), a zero inline count for a
    /// lone `count`, or without aggregate functions the sentinel of `executeImplBatchNoAggregates`. Generic,
    /// so that its body compiles only for the methods that keep the slot.
    const auto emplace_null_key = [&](auto & data) -> AggregateDataPtr &
    {
        if (!data.hasNullKeyData())
        {
            data.hasNullKeyData() = true;
            data.getNullKeyData() = nullptr;
            if (params.aggregates_size == 0)
            {
                data.getNullKeyData() = reinterpret_cast<AggregateDataPtr>(0x1);
            }
            else if (!is_simple_count)
            {
                /// Local state growth belongs to this table, while the staging runs under the aggregator's
                /// tracker.
                std::optional<MemoryTrackerSwitcher> local_scope;
                if (local_memory_tracker)
                    local_scope.emplace(local_memory_tracker);
                AggregateDataPtr place = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                createAggregateStates(place);
                data.getNullKeyData() = place;
            }
        }
        return data.getNullKeyData();
    };

    /// With a row selection, the misses of a key that combine into its partition's tail stage one record, of
    /// their best row by the ranks of their blocks (see `AdaptiveMissCombiner`). When the selection counts
    /// rows, the record also counts them.
    const AdaptiveRowSelection * selection = adaptive_row_selection.get();
    if (selection)
    {
        const IColumn & ordering_column
            = *aggregate_instructions[selection->ordering_aggregate].arguments[selection->ordering_argument];
        adaptive.row_selection_ranks.startBlock(*selection, ordering_column, row_begin, row_end);
    }

    if (all_keys_are_const)
    {
        /// Applies the whole range, whose rows all carry one key, to the key's place in the table, mirroring
        /// the ordinary all-keys-are-const handling.
        const auto add_range_to_place = [&](AggregateDataPtr & place)
        {
            if (is_simple_count)
            {
                getInlineCountState(place) += row_end - row_begin;
            }
            else if (params.aggregates_size)
            {
                /// Local state growth belongs to this table, while the staging runs under the aggregator's tracker.
                std::optional<MemoryTrackerSwitcher> local_scope;
                if (local_memory_tracker)
                    local_scope.emplace(local_memory_tracker);

                for (size_t i = 0; i < aggregate_functions.size(); ++i)
                {
                    AggregateFunctionInstruction * inst = aggregate_instructions + i;
                    ProfileEvents::increment(ProfileEvents::AggregationOptimizedEqualRangesOfKeys);
                    addBatchSinglePlace(row_begin, row_end, inst, place + inst->state_offset, aggregates_pool);
                }
            }
        };

        /// A NULL key goes to its slot without a lookup.
        if constexpr (keeps_null_key_slot<LocalMethod>)
        {
            if (local_find_state.isNullAt(0))
            {
                add_range_to_place(emplace_null_key(local_method.data));
                return;
            }
        }

        auto && key_holder = local_find_state.getKeyHolder(0, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);

        auto found = local_method.data.find(key, hash);
        if (found)
        {
            add_range_to_place(found->getMapped());
        }
        else
        {
            misses.setKeysConst();
            /// When the records combine, the block's misses go through the tails of the producer's partitions
            /// or past them, as one chunk (see `ConstantKeyMissProbe`): a count record counts the block's
            /// rows, a record without aggregate functions carries the key alone, and the record of a row
            /// selection takes the block's best row.
            using KeepKeyBytes = std::remove_cv_t<decltype(keep_key_bytes)>;
            if (is_simple_count)
                stageConstantKeyMisses<RecordKey, AdaptiveMissCombine::counts, KeepKeyBytes>(
                    adaptive, keep_key_bytes, selection, hash, key, row_begin, row_end);
            else if (selection)
                stageConstantKeyMisses<RecordKey, AdaptiveMissCombine::selection, KeepKeyBytes>(
                    adaptive, keep_key_bytes, selection, hash, key, row_begin, row_end);
            else if (params.aggregates_size == 0)
                stageConstantKeyMisses<RecordKey, AdaptiveMissCombine::keys, KeepKeyBytes>(
                    adaptive, keep_key_bytes, selection, hash, key, row_begin, row_end);
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

    /// The records of a lone `count` and those without aggregate functions carry no arguments, so a miss of
    /// the key of its partition's tail combines into the tail (see `AdaptivePartitionTails`).
    if (is_simple_count || params.aggregates_size == 0)
    {
        const AdaptiveLocalProbes probes = is_simple_count
            ? collectCombinedMisses</*with_count=*/true, RecordKey>(
                  local_method, local_find_state, row_begin, row_end, scratch_pool, adaptive, bypass_local_probe, stage_misses)
            : collectCombinedMisses</*with_count=*/false, RecordKey>(
                  local_method, local_find_state, row_begin, row_end, scratch_pool, adaptive, bypass_local_probe, stage_misses);
        if constexpr (keeps_null_key_slot<LocalMethod>)
        {
            if (probes.null_rows)
            {
                AggregateDataPtr & place = emplace_null_key(local_method.data);
                if (is_simple_count)
                    getInlineCountState(place) += probes.null_rows;
            }
        }
        record_local_probes(probes.hits, row_end - row_begin - probes.null_rows);
        stage_misses();
        return;
    }

    /// With the probe bypassed there is no hit, so there are no places to record, unless the block may have
    /// rows of the NULL key.
    if (bypass_local_probe && !local_find_state.mayHaveNullRows())
    {
        const AdaptiveLocalProbes probes = collectArgumentMisses</*record_places=*/false, RecordKey>(
            local_method, local_find_state, row_begin, row_end, scratch_pool, adaptive, bypass_local_probe,
            /*places_data=*/nullptr, emplace_null_key, stage_misses);
        record_local_probes(probes.hits, row_end - row_begin);
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

    const AdaptiveLocalProbes probes = collectArgumentMisses</*record_places=*/true, RecordKey>(
        local_method, local_find_state, row_begin, row_end, scratch_pool, adaptive, bypass_local_probe, places.get(),
        emplace_null_key, stage_misses);
    record_local_probes(probes.hits, row_end - row_begin - probes.null_rows);
    stage_misses();

    /// With no local hits and no NULL rows every place is null and the batch pass would only skip rows; the
    /// staged records carry the block's whole contribution.
    if (probes.hits + probes.null_rows != 0)
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

template <typename Method, typename State>
AdaptiveStagedBytes NO_INLINE Aggregator::appendDelayedRecords(
    const AggregateFunctionInstruction * aggregate_instructions,
    AdaptiveAggregationProducer & adaptive,
    State & local_find_state,
    Arena & scratch_pool) const
{
    using RecordKey = AdaptiveRecordKey<Method>;

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
    /// The size of a byte-staged key, or 0 for a fixed-width key, whose formats know its stored width.
    const auto key_size_of = [&](size_t record) -> size_t
    {
        if constexpr (adaptive_key_stages_bytes<RecordKey>)
            return staged_key_sizes[record];
        else
            return 0;
    };

    /// The tails of an aggregation whose records combine learn where the records of their descriptors went
    /// (see `AdaptivePartitionTails::recordsWritten`), while they are open: closed tails take no positions
    /// (see `AdaptivePartitionTails::startBatch`). The loops below are compiled with and without the
    /// positions: the appends wait on memory, so every instruction that a record does not need shortens how
    /// many records' cache misses overlap.
    auto * tails = adaptive.tails ? &assert_cast<AdaptivePartitionTails<RecordKey> &>(*adaptive.tails) : nullptr;
    char ** const record_positions = tails ? tails->startBatch(hashes, total) : nullptr;

    /// The appends go to random partitions, so the loops below prefetch the append position of
    /// the record `adaptive_append_prefetch_distance` ahead.
    const auto prefetch_append = [&partitions, layout, hashes, total](size_t record) ALWAYS_INLINE
    {
        if (record + adaptive_append_prefetch_distance < total)
            partitions.prefetchAppend(layout.partitionOf(hashes[record + adaptive_append_prefetch_distance]));
    };

    /// The records are written in staging order, so they take the key bytes that the batch kept in their order, and the
    /// records of a constant key all take its one copy. A fixed-width key is copied whole, though its record may store
    /// fewer of its bytes (see `StagedFixedKey`): the bytes past the stored ones land where the record's later fields,
    /// the next record or the chunk's padding go, as the overflow of `copyPaddedKeyBytes` does.
    const auto batch_keeps_key_bytes = adaptiveMissesKeepKeyBytes<RecordKey>(local_find_state);
    const char * kept_key_bytes = staged_keys;
    const auto write_key = [&](size_t record, [[maybe_unused]] size_t key_size, char * to, [[maybe_unused]] const auto & format) ALWAYS_INLINE
    {
        if constexpr (is_keys_and_packed_string<RecordKey>)
        {
            /// The state reads the packed values and the string in place, and the format writes them (see
            /// `StagedKeysAndPackedStringKey::writeKey`).
            const auto [keys, str] = local_find_state.getKeyParts(key_row_of(record));
            chassert(sizeof(keys) + str.size() == key_size);
            format.writeKey(to, keys, str);
        }
        else if constexpr (adaptive_key_stages_bytes<RecordKey>)
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

    /// The merge reads back the routing hash of a byte-staged key and the whole value of a fixed-width key from the
    /// bytes that the record stores.
    const auto check_record = [&](const auto & format, const char * record, [[maybe_unused]] size_t i, UInt64 hash)
    {
        if constexpr (adaptive_key_stages_bytes<RecordKey>)
            chassert(format.routingHash(record) == hash);
        else
            chassert(format.readKey(record, hash) == unalignedLoad<RecordKey>(staged_keys + i * sizeof(RecordKey)));
    };

    size_t key_bytes = 0;
    size_t record_bytes = 0;

    const auto append_key_records = [&]<typename Record, bool with_positions>(const Record & format, std::bool_constant<with_positions>) ALWAYS_INLINE
    {
        for (size_t i = 0; i < total; ++i)
        {
            prefetch_append(i);
            const UInt64 hash = hashes[i];
            const size_t partition = layout.partitionOf(hash);
            const size_t key_size = key_size_of(i);
            UInt32 rest = 0;
            UInt32 count = 0;
            if constexpr (Record::with_count)
            {
                rest = misses.getMultiplicities()[i];
                count = std::min(rest, Record::max_count);
            }
            const size_t bytes = format.bytesForKey(key_size, count);
            char * record = partitions.append(partition, bytes);
            char * const stored_key = format.writeHeader(record, hash, key_size, count);
            write_key(i, key_size, stored_key, format);
            const size_t stored_key_size = format.keyBytes(key_size);
            check_record(format, record, i, hash);
            key_bytes += stored_key_size;
            record_bytes += bytes;

            /// A count past the format's `max_count`, the largest that one record holds, continues in further
            /// records of the key. The last of them is the partition's last record, which the tail of the key
            /// may grow.
            if constexpr (Record::with_count)
            {
                for (rest -= count; rest; rest -= count)
                {
                    count = std::min(rest, Record::max_count);
                    const size_t extra_bytes = format.bytesForKey(key_size, count);
                    char * extra = partitions.append(partition, extra_bytes);
                    memcpy(format.writeHeader(extra, hash, key_size, count), stored_key, stored_key_size);
                    key_bytes += stored_key_size;
                    record_bytes += extra_bytes;
                    if constexpr (with_positions)
                        record = extra;
                }
            }
            if constexpr (with_positions)
                record_positions[i] = record;
        }
    };

    const auto append_argument_records = [&]<typename Record, bool with_positions>(const Record & format, std::bool_constant<with_positions>) ALWAYS_INLINE
    {
        const auto & argument_layout = *adaptive_argument_layout;
        /// The multiplicities of the batch, or null when every record of the batch stands for one row (see
        /// `AdaptiveMissBatch::hasMultiplicities`). A local of the loop, which it keeps in a register: what
        /// it reads through a reference it loads again after every store into a record.
        chassert(!misses.hasMultiplicities() || misses.getMultiplicities().size() == total);
        [[maybe_unused]] const UInt32 * const batch_multiplicities = misses.hasMultiplicities() ? misses.getMultiplicities().data() : nullptr;

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

        /// A sparse column, which an aggregate of one argument consumes as it is, is read through its values.
        /// A dense copy of the column would also copy the values of the rows that hit the local table. While
        /// the source rows of the records ascend, one pass of an iterator along its offsets finds the value
        /// of every record. A row selection may have moved a record to a later row (see
        /// `AdaptiveMissBatch::setSourceRow`), and then each row is looked up in the offsets.
        std::vector<PaddedPODArray<UInt32>> value_rows(argument_layout.num_positions);
        const auto read_through_values = [&](size_t position)
        {
            ArgumentSource & argument = arguments[position];
            const auto * sparse = typeid_cast<const ColumnSparse *>(argument.column);
            if (!sparse)
                return;
            auto & rows = value_rows[position];
            rows.resize(total);
            if (std::is_sorted(source_rows, source_rows + total))
            {
                auto iterator = sparse->getIterator(source_rows[0]);
                for (size_t i = 0; i < total; ++i)
                {
                    iterator.advanceToRow(source_rows[i]);
                    rows[i] = static_cast<UInt32>(iterator.getValueIndex());
                }
            }
            else
            {
                for (size_t i = 0; i < total; ++i)
                    rows[i] = static_cast<UInt32>(sparse->getValueIndex(source_rows[i]));
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
            for (size_t i = batch_begin; i < batch_end; ++i)
            {
                prefetch_append(i);
                const UInt64 hash = hashes[i];
                const size_t partition = layout.partitionOf(hash);
                const size_t key_size = key_size_of(i);

                size_t variable_bytes = 0;
                const char * scratch_begin = nullptr;
                size_t scratch_bytes = 0;
                if constexpr (Record::with_variable_arguments)
                {
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
                }

                const size_t bytes = format.bytesFor(key_size, variable_bytes);
                char * record = partitions.append(partition, bytes);
                argument_rows[i - batch_begin] = record;
                if constexpr (with_positions)
                    record_positions[i] = record;

                /// The multiplicity goes into the record here, with the key, for which the loop waits on the
                /// record's memory anyway: a pass of its own over the records would wait on it once more.
                if constexpr (Record::with_multiplicity)
                    format.writeMultiplicity(record, batch_multiplicities ? batch_multiplicities[i] : 1);

                /// The key goes before the variable-size arguments, which overwrite whatever its copy scribbled past
                /// it.
                char * key = format.writeHeader(record, hash, key_size, bytes);
                write_key(i, key_size, key, format);
                const size_t stored_key_size = format.keyBytes(key_size);

                if constexpr (Record::with_variable_arguments)
                {
                    char * variable = key + stored_key_size;
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
                    chassert(variable == record + bytes);
                    if (scratch_bytes)
                        argument_scratch->rollback(scratch_bytes);
                }
                check_record(format, record, i, hash);

                key_bytes += stored_key_size;
                record_bytes += bytes;
            }

            const size_t batch_rows = batch_end - batch_begin;
            for (const auto & field : argument_layout.fixed_fields)
            {
                const auto & [column, rows] = arguments[field.position];
                column->gatherSelectedIntoRows(
                    {rows + batch_begin, batch_rows},
                    {argument_rows.data(), batch_rows},
                    field.offset,
                    /*null_map=*/ nullptr);
            }
        }
    };

    visitStagedRecordFormat<Method>(
        adaptive_argument_layout.get(),
        is_simple_count,
        adaptive_packed_key_bytes,
        [&]<typename Record>(const Record & format) ALWAYS_INLINE
        {
            const auto append = [&]<bool with_positions>(std::bool_constant<with_positions> positions) ALWAYS_INLINE
            {
                if constexpr (Record::with_arguments)
                    append_argument_records(format, positions);
                else
                    append_key_records(format, positions);
            };
            if (record_positions)
            {
                append(std::true_type{});
                tails->recordsWritten(format);
            }
            else
                append(std::false_type{});
        });
    /// The kernel and the writer decide on keeping the key bytes from the same state, and every kept byte belongs to
    /// exactly one record, or to all the records of a constant key.
    chassert(!batch_keeps_key_bytes
        || misses.getKeys().size() == (keys_are_const ? key_size_of(0) : static_cast<size_t>(kept_key_bytes - staged_keys)));

    return {.key_bytes = key_bytes, .record_bytes = record_bytes};
}

}
