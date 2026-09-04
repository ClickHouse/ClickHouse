/// The method-specialized kernels of the adaptive aggregation: the frozen consume path, the
/// staging of missed rows, and the bucket drains. They are member templates of `Aggregator`
/// (defined here rather than in `Aggregator.cpp`, following `ClientBaseOptimizedParts.cpp`),
/// dispatched over the aggregation-method variants.

#include <algorithm>
#include <bit>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/memcpySmall.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <base/arithmeticOverflow.h>
#include <base/memcmpSmall.h>
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
    extern const Event AdaptiveAggregationSealNormalizations;
    extern const Event AdaptiveAggregationDrainedRecords;
    extern const Event AdaptiveAggregationPressureSweeps;
    extern const Event AdaptiveAggregationPressureDrainedRecords;
    extern const Event AdaptiveAggregationResidueReleases;
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
    /// String-like keys stage their bytes: a packed reference copied as a plain value would
    /// carry a pointer into the source block, which dies when the publish compacts the
    /// arguments and releases it. The staged form of both string kinds is the raw characters,
    /// and the drain rebuilds the table's key from them (the pressure-time drain additionally
    /// persists the bytes into its arena; the merge-time drain borrows them).
    template <typename Key>
    constexpr bool adaptive_key_stages_bytes = std::is_same_v<Key, std::string_view> || std::is_same_v<Key, PackedStringRef>;

    template <typename Key>
    ALWAYS_INLINE std::string_view adaptiveStagedKeyBytes(const Key & key)
    {
        if constexpr (std::is_same_v<Key, PackedStringRef>)
            return static_cast<std::string_view>(key);
        else
            return key;
    }

    /// How far past a key's bytes a reader may touch. The overflow-tolerant small copy and
    /// compare primitives access up to 15 bytes past the end, which is only legal for bytes
    /// living in padded containers (column chars, arenas, the staged arrays); an exact-size
    /// allocation forbids them.
    enum class ReadablePadding
    {
        Exact,
        AtLeast15Bytes,
    };

    struct KeyBytesRef
    {
        std::string_view bytes;
        ReadablePadding padding;
    };

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
            callback(KeyBytesRef{std::string_view(data, size), ReadablePadding::AtLeast15Bytes});
        }
        else if constexpr (adaptive_key_stages_bytes<SharedKey>)
        {
            auto && key_holder = state.getKeyHolder(row, scratch);
            callback(KeyBytesRef{adaptiveStagedKeyBytes(keyHolderGetKey(key_holder)), ReadablePadding::Exact});
            keyHolderDiscardKey(key_holder);
        }
        else
        {
            auto && key_holder = state.getKeyHolder(row, scratch);
            const SharedKey widened = keyHolderGetKey(key_holder);
            keyHolderDiscardKey(key_holder);
            callback(KeyBytesRef{std::string_view(reinterpret_cast<const char *>(&widened), sizeof(widened)), ReadablePadding::Exact});
        }
    }

    /// Compare a staged key (always in a padded container) with candidate bytes of the same
    /// size. The overflow-tolerant primitive reads past both ends, so it is gated on the
    /// candidate's padding; small keys are where it beats a libc call.
    bool ALWAYS_INLINE stagedKeyEquals(const char * staged, const KeyBytesRef & key)
    {
        if (key.padding == ReadablePadding::AtLeast15Bytes && key.bytes.size() <= 64)
            return memequalSmallAllowOverflow15(staged, key.bytes.size(), key.bytes.data(), key.bytes.size());
        return memcmp(staged, key.bytes.data(), key.bytes.size()) == 0;
    }

    /// Copy candidate bytes into staged (padded) storage, honoring the source's padding. The
    /// overflow-tolerant branch also writes up to 15 bytes past the destination, so the callers
    /// must append in increasing byte order (the scribble lands in space the next append
    /// overwrites); a caller that scatters must use a plain bounded copy instead.
    void ALWAYS_INLINE copyStagedKeyBytes(char * staged, const KeyBytesRef & key)
    {
        if (key.bytes.empty())
            return;
        if (key.padding == ReadablePadding::AtLeast15Bytes && key.bytes.size() <= 64)
            memcpySmallAllowReadWriteOverflow15(staged, key.bytes.data(), key.bytes.size());
        else
            memcpy(staged, key.bytes.data(), key.bytes.size());
    }

    /// The count-record dedup primitive shared by the publish and seal walks. A duplicate key
    /// can only be one of its group's survivors, the records staged in [group_begin, out) with
    /// the same few hash bits (usually zero or one): merge the run lengths instead of staging
    /// another copy of the key, with equal hashes of distinct keys split by the byte
    /// comparison. Otherwise the record is appended at `out` and the cursors advance.
    ///
    /// The overflow-split policy lives here and only here: a survivor whose multiplicity would
    /// exceed 32 bits is skipped, because a later survivor of the same key (from a previous
    /// overflow split) may still have capacity, and otherwise the record starts a fresh
    /// survivor of the same key.
    void ALWAYS_INLINE mergeOrAppendStagedCount(
        DB::StagedChunk::StagedKeys & keys,
        DB::PaddedPODArray<UInt32> & multiplicities,
        const UInt64 hash,
        const KeyBytesRef & key,
        const UInt32 multiplicity,
        const size_t group_begin,
        size_t & out,
        UInt64 & byte_pos)
    {
        const size_t size = key.bytes.size();
        for (size_t j = group_begin; j < out; ++j)
        {
            if (keys.routing_hashes[j] != hash)
                continue;
            /// A fixed-size-key chunk needs no size comparison: every record is `size` wide.
            if (keys.fixed_key_size)
            {
                if (!stagedKeyEquals(keys.key_bytes.data() + j * keys.fixed_key_size, key))
                    continue;
            }
            else
            {
                const UInt64 j_end = (j + 1 == out) ? byte_pos : keys.key_offsets[j + 1];
                if (j_end - keys.key_offsets[j] != size || !stagedKeyEquals(keys.key_bytes.data() + keys.key_offsets[j], key))
                    continue;
            }
            if (static_cast<UInt64>(multiplicities[j]) + multiplicity > std::numeric_limits<UInt32>::max())
                continue;
            multiplicities[j] += multiplicity;
            return;
        }

        keys.routing_hashes[out] = hash;
        multiplicities[out] = multiplicity;
        if (!keys.fixed_key_size)
            keys.key_offsets[out] = byte_pos;
        copyStagedKeyBytes(keys.key_bytes.data() + byte_pos, key);
        byte_pos += size;
        ++out;
    }

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
        if constexpr (adaptive_key_stages_bytes<Key>)
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
    AggregateFunctionInstruction *,
    AdaptiveAggregationProducer & adaptive,
    bool all_keys_are_const) const
{
    Arena scratch_pool;

    typename LocalMethod::StateNoCache local_find_state(key_columns, key_sizes, aggregation_state_cache);

    /// The kernel runs only while the producer is frozen, and phase transitions happen between
    /// blocks, so the reference stays valid for the whole block.
    auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    const bool bypass_local_probe = frozen.bypass_local_probe;

    auto stage_miss = [&]([[maybe_unused]] const auto & key, UInt64 hash, size_t row)
    {
        adaptive.miss_source_rows.push_back(static_cast<UInt32>(row));
        adaptive.miss_hashes.push_back(hash);
        adaptive.miss_buckets.push_back(static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash)));
        if constexpr (adaptive_key_stages_bytes<typename SharedMethod::Key>)
            adaptive.miss_key_sizes.push_back(adaptiveStagedKeyBytes(key).size());
    };

    if (all_keys_are_const)
    {
        auto && key_holder = local_find_state.getKeyHolder(0, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);

        /// The whole range carries one key and a set stores a key once, so a single record stands
        /// for it however many rows it spans.
        if (!local_method.data.find(key, hash))
        {
            stage_miss(key, hash, row_begin);
            publishDelayedRecords<typename SharedMethod::Key>(
                columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/false, /*key_row_override=*/0);
        }
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
            stage_miss(key, hash, i);

        keyHolderDiscardKey(key_holder);
    }

    if (!frozen.bypass_local_probe)
    {
        frozen.sampled_hits += hits;
        frozen.sampled_rows += row_end - row_begin;
        if (frozen.sampled_rows >= adaptive_bypass_sample_rows
            && frozen.sampled_hits * adaptive_bypass_hit_rate_inverse < frozen.sampled_rows)
        {
            frozen.bypass_local_probe = true;
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);
        }
    }

    publishDelayedRecords<typename SharedMethod::Key>(
        columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);
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
                adaptive.miss_hashes.push_back(hash);
                adaptive.miss_multiplicities.push_back(static_cast<UInt32>(row_end - row_begin));
                if constexpr (adaptive_key_stages_bytes<typename SharedMethod::Key>)
                    adaptive.miss_key_sizes.push_back(adaptiveStagedKeyBytes(key).size());
                adaptive.miss_buckets.push_back(bucket);
            }
            else
            {
                for (size_t i = row_begin; i < row_end; ++i)
                {
                    adaptive.miss_source_rows.push_back(static_cast<UInt32>(i));
                    adaptive.miss_hashes.push_back(hash);
                    adaptive.miss_buckets.push_back(bucket);
                    if constexpr (adaptive_key_stages_bytes<typename SharedMethod::Key>)
                        adaptive.miss_key_sizes.push_back(adaptiveStagedKeyBytes(key).size());
                }
            }
            publishDelayedRecords<typename SharedMethod::Key>(
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

            bool run_continues = !adaptive.miss_hashes.empty() && adaptive.miss_hashes.back() == hash;
            if constexpr (std::is_same_v<typename SharedMethod::Key, std::string_view>)
                run_continues = run_continues && stable_key_views && staged_key == last_staged_key;
            else
                run_continues = run_continues && staged_key == last_staged_key;

            if (run_continues)
            {
                ++adaptive.miss_multiplicities.back();
            }
            else
            {
                adaptive.miss_hashes.push_back(hash);
                adaptive.miss_multiplicities.push_back(1);
                /// Fixed-size keys stage no size: it is a compile-time constant the publish
                /// substitutes, so the hot staging loop skips a dead store per record.
                if constexpr (adaptive_key_stages_bytes<typename SharedMethod::Key>)
                    adaptive.miss_key_sizes.push_back(adaptiveStagedKeyBytes(staged_key).size());

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
                adaptive.miss_source_rows.push_back(static_cast<UInt32>(i));
                adaptive.miss_buckets.push_back(static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash)));
            }
            keyHolderDiscardKey(key_holder);
        }
        update_bypass_sampling(hits, row_end - row_begin);
        publishDelayedRecords<typename SharedMethod::Key>(columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/true);
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
            adaptive.miss_source_rows.push_back(static_cast<UInt32>(i));
            adaptive.miss_hashes.push_back(hash);
            adaptive.miss_buckets.push_back(static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash)));

            if constexpr (adaptive_key_stages_bytes<typename SharedMethod::Key>)
                adaptive.miss_key_sizes.push_back(adaptiveStagedKeyBytes(key).size());
            keyHolderDiscardKey(key_holder);
        }
        return hits;
    };

    if (params.aggregates_size == 0)
    {
        const size_t hits = probe_rows.template operator()<false>(nullptr);
        update_bypass_sampling(hits, row_end - row_begin);
        publishDelayedRecords<typename SharedMethod::Key>(columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);
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
    publishDelayedRecords<typename SharedMethod::Key>(columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);

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
void NO_INLINE Aggregator::buildDeduplicatedCountChunk(
    StagedChunk & block,
    AdaptiveAggregationProducer & adaptive,
    State & local_find_state,
    Arena & scratch_pool,
    std::optional<UInt32> key_row_override) const
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;
    const size_t total = adaptive.miss_hashes.size();

    /// Group the records by (bucket, a few extra hash bits): a duplicate key always lands in
    /// the same group, so the dedup below only compares within a group, and group-id order is
    /// bucket-major, which is the block's slice layout. The group count scales with the batch
    /// (~16 records per group), so the histogram stays cache-resident and small batches do not
    /// pay for counters they cannot fill. A bypassed pass (see `DedupProductivity`) degrades
    /// the grouping to plain buckets and the dedup scan below to a straight append.
    const bool dedup = adaptive.publish_dedup.shouldDedup();
    const UInt32 sub_bits = dedup ? std::min<UInt32>(8, std::bit_width(total >> 12)) : 0;
    const size_t num_groups = num_buckets << sub_bits;

    auto & grouped_indexes = adaptive.grouped_index_scratch;
    grouped_indexes.resize(total);
    auto & offsets = adaptive.group_offsets_scratch;
    auto & cursor = adaptive.group_cursor_scratch;
    offsets.assign(num_groups + 1, 0);
    cursor.resize(num_groups);

    const auto group_of = [&](size_t i) -> UInt32
    {
        const UInt32 bucket = adaptive.miss_buckets[i];
        return (bucket << sub_bits) | (static_cast<UInt32>(adaptive.miss_hashes[i] >> 10) & ((1u << sub_bits) - 1));
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
    if constexpr (adaptive_key_stages_bytes<SharedKey>)
        for (const auto size : adaptive.miss_key_sizes)
            total_bytes += size;
    else
        total_bytes = total * sizeof(SharedKey);

    auto & keys = block.keys;
    auto & multiplicities = block.payload.emplace<StagedChunk::CountPayload>().multiplicities;
    if constexpr (!adaptive_key_stages_bytes<SharedKey>)
        keys.fixed_key_size = sizeof(SharedKey);
    keys.routing_hashes.resize(total);
    multiplicities.resize(total);
    if constexpr (adaptive_key_stages_bytes<SharedKey>)
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
            const UInt64 hash = adaptive.miss_hashes[idx];
            const size_t size = [&]
            {
                if constexpr (adaptive_key_stages_bytes<SharedKey>)
                    return adaptive.miss_key_sizes[idx];
                else
                    return sizeof(SharedKey);
            }();
            const size_t key_row = key_row_override ? *key_row_override : adaptive.miss_source_rows[idx];

            /// The key bytes are read straight from the hashing state's column when it exposes
            /// them: the generic key holder of the packed method would re-pack the key and
            /// re-compute its content hash per record, and the staged arrays already hold both.
            /// All byte uses happen inside the holder's lifetime (see `withStagedKeyBytes`).
            /// A bypassed pass hands the append an empty candidate range, so nothing is scanned.
            withStagedKeyBytes<SharedKey>(
                local_find_state,
                key_row,
                size,
                scratch_pool,
                [&](const KeyBytesRef & key)
                {
                    mergeOrAppendStagedCount(
                        keys, multiplicities, hash, key, adaptive.miss_multiplicities[idx], dedup ? group_out_begin : out, out, byte_pos);
                });
        }
    }

    keys.bucket_offsets[num_buckets] = static_cast<UInt32>(out);
    if constexpr (adaptive_key_stages_bytes<SharedKey>)
    {
        keys.key_offsets[out] = byte_pos;
        keys.key_offsets.resize(out + 1);
    }

    keys.routing_hashes.resize(out);
    multiplicities.resize(out);
    keys.key_bytes.resize(byte_pos);

    if (dedup)
        adaptive.publish_dedup.record(total, out);
}

template <typename SharedKey, typename State>
void NO_INLINE Aggregator::buildBucketGroupedAggregateChunk(
    StagedChunk & block,
    const Columns & columns,
    AdaptiveAggregationProducer & adaptive,
    State & local_find_state,
    Arena & scratch_pool,
    std::optional<UInt32> key_row_override) const
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;
    const size_t total = adaptive.miss_hashes.size();
    auto & keys = block.keys;

    auto & payload = block.payload.emplace<StagedChunk::AggregatePayload>();

    /// The sizes are exact and final, and the chunk can sit on a backlog for the rest of the
    /// query, so the arrays are sized without the power-of-two growth headroom.
    keys.routing_hashes.resize_exact(total);
    if constexpr (adaptive_key_stages_bytes<SharedKey>)
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
        if constexpr (adaptive_key_stages_bytes<SharedKey>)
            return adaptive.miss_key_sizes[record];
        else
            return sizeof(SharedKey);
    };

    std::array<UInt32, num_buckets> cursor{};
    std::array<UInt64, num_buckets> byte_cursor{};
    for (size_t i = 0; i < total; ++i)
    {
        ++cursor[adaptive.miss_buckets[i]];
        byte_cursor[adaptive.miss_buckets[i]] += staged_key_size(i);
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
    if constexpr (adaptive_key_stages_bytes<SharedKey>)
        keys.key_offsets[total] = byte_offset;

    keys.key_bytes.resize_exact(byte_offset);

    /// The records' source row numbers in bucket-grouped order: the gather indexes that compact
    /// the argument columns below. A zero-aggregate block stages keys only, so it needs none.
    ColumnUInt32::MutablePtr gather_indexes;
    UInt32 * gather_data = nullptr;
    if (params.aggregates_size != 0)
    {
        gather_indexes = ColumnUInt32::create();
        gather_indexes->getData().resize_exact(total);
        gather_data = gather_indexes->getData().data();
    }

    for (size_t i = 0; i < total; ++i)
    {
        const auto b = adaptive.miss_buckets[i];
        const auto pos = cursor[b]++;
        keys.routing_hashes[pos] = adaptive.miss_hashes[i];

        if (gather_data)
            gather_data[pos] = adaptive.miss_source_rows[i];

        const auto size = staged_key_size(i);
        const auto byte_pos = byte_cursor[b];
        byte_cursor[b] += size;
        if constexpr (adaptive_key_stages_bytes<SharedKey>)
            keys.key_offsets[pos] = byte_pos;

        /// The same byte extraction the count path uses: states that expose their padded
        /// column buffers hand the bytes out directly (in particular, the packed-string method
        /// does not rebuild the key, which would re-hash its content per record). The copy is
        /// a plain bounded memcpy, NOT copyStagedKeyBytes: records scatter into bucket-grouped
        /// positions, so an overflow-tolerant write would stomp neighbors that are already in
        /// place. The guard also keeps the empty packed key's null data pointer away from
        /// memcpy, which declares its sources nonnull.
        const size_t key_row = key_row_override ? *key_row_override : adaptive.miss_source_rows[i];
        withStagedKeyBytes<SharedKey>(
            local_find_state,
            key_row,
            size,
            scratch_pool,
            [&](const KeyBytesRef & key)
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
            /// The gather stays on the cheap representation: a constant is resized and a
            /// sparse column is gathered in its sparse form, instead of materializing the
            /// whole block just to gather the staged subset from it. The gathered column is
            /// then normalized to the dense form the drain consumes (the representation
            /// wrappers stripped recursively, then `LowCardinality`), so the chunk stores
            /// exactly what will be drained: the thaw estimate and the pinned memory
            /// accounting measure the real payload, the publish-time preparation wires the
            /// columns directly instead of pinning a second, dense copy next to the wrapper,
            /// and a gathered `LowCardinality` no longer holds the source block's dictionary
            /// alive until the merge.
            ColumnPtr gathered = isColumnConst(*columns[position])
                ? columns[position]->cloneResized(total)
                : columns[position]->index(*gather_indexes, 0);
            ColumnPtr normalized = recursiveRemoveLowCardinality(gathered->convertToFullIfWrapped());
            if (normalized.get() != gathered.get())
                ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSealNormalizations);
            payload.argument_columns[position] = std::move(normalized);
        }
}

template <typename SharedKey, typename State>
void NO_INLINE Aggregator::publishDelayedRecords(
    const Columns & columns,
    size_t num_rows,
    AdaptiveAggregationProducer & adaptive,
    State & local_find_state,
    Arena & scratch_pool,
    bool counts_only,
    std::optional<UInt32> key_row_override) const
{
    const size_t total = adaptive.miss_hashes.size();
    if (!total)
        return;

    if (num_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adaptive aggregation got a block of {} rows; row numbers are 32-bit.", num_rows);

    auto & shared = *adaptive.session;

    /// Thawing is the adaptive aggregation standing down globally: when the staged stream
    /// proves to keep repeating the same missing keys instead of bringing rare ones, every
    /// thread returns to ordinary insertion for good. A frozen table thaws; a thread still
    /// learning stops trying to freeze. Staging such a stream re-copies a repeated key's
    /// bytes on every occurrence, while an unfrozen table would absorb the repeats as cheap
    /// in-place updates.
    ///
    /// The verdict is evaluated over totals shared by all threads; the tuning constants
    /// hold the calibration:
    ///
    ///     wasted bytes per distinct key = (repeat - 1) * bytes per record
    ///                                   > adaptive_thaw_wasted_bytes_per_key
    ///
    /// Here repeat = thaw_sampled_records / distinct_sampled_hashes, and bytes per record =
    /// staged_bytes / staged_records. A key's first record is the price of storing it once;
    /// each repeat wastes one record's bytes, so heavy records tolerate few repeats and tiny
    /// ones many. Until the verdict fires, every publish folds its batch into the shared
    /// evidence and re-evaluates, so the thread whose batch tips the totals over the bound
    /// fires for everyone by setting `thaw_all`, once `staged_records` has reached the
    /// `adaptive_thaw_min_staged_records` evidence floor. A publish updates:
    ///
    /// - `staged_records` grows by the batch's record count.
    /// - `staged_bytes` grows by the batch's estimated footprint, computed below as
    ///   `batch_bytes`. It counts the key bytes as the kernel staged them, the variable-width
    ///   aggregate arguments at their gathered sizes (the sealed chunk's columns hold exactly
    ///   the staged rows, so a wide tail behind a narrow frequent head is charged its real
    ///   width rather than the block's average), and the per-record bookkeeping the chunk
    ///   stores (the eight-byte routing hash, plus an eight-byte key offset only for
    ///   byte-staged keys; fixed keys have no offsets). A column read by several aggregates is staged
    ///   once, so it is counted once; a count batch stages a four-byte run length instead of
    ///   arguments.
    ///   The estimate is taken before the count deduplication (which merges a batch's
    ///   repeats of one key into a single record with a run length), so it charges every
    ///   staged record. That is deliberate: `staged_records` also counts the records before
    ///   deduplication, and the verdict's bytes per record is `staged_bytes` divided by
    ///   `staged_records`, so the two counters must describe the same set of records for
    ///   the ratio to mean anything.
    ///   Variable-width arguments count in full because staging such a value pays real work
    ///   at every step. The seal gathers it out of the block into the staged column (a copy),
    ///   the staged chunk pins that memory until the merge drains it, and updating the
    ///   aggregate state from it copies the value once more (a string min keeps its own copy
    ///   of the winning value). A repeated key pays all of that on every occurrence, where
    ///   an unfrozen table would have paid a single in-place state update, so each repeat of
    ///   a heavy value is genuine waste.
    ///   Fixed-width arguments are deliberately not counted because their staging copy is a
    ///   few bytes and the drain consumes the staged batch with the same vectorized batch
    ///   executor the scan would have used on the original block. Deferring such values
    ///   moves the work without multiplying it, so their staging costs about what their
    ///   consumption saves. Charging them would fire the thaw on streams where staging is in
    ///   fact profitable. The measured anchor is a stream of five UInt64 arguments at repeat
    ///   10: it stays a clear adaptive win, and counting its forty fixed bytes per record
    ///   would have thawed it.
    /// - The sampler receives the batch's routing hashes matching `hash & 0xFF == 0`, about
    ///   total / 256 of them, collected outside the lock. `thaw_sampled_records` counts
    ///   every sampled occurrence; `distinct_sampled_hashes` collapses a key's repeats onto
    ///   one entry across all threads, so their ratio estimates the stream's repeat factor
    ///   independently of how the keys spread over the threads.
    ///
    /// The verdict lands at each thread's next between-blocks check; a learning thread about
    /// to freeze also checks it at the crossing, so no table freezes against it. The current
    /// records are still published: their rows were deferred by the frozen kernel and only
    /// the drain will aggregate them.
    auto block = std::make_shared<StagedChunk>();
    auto & keys = block->keys;

    if (counts_only)
    {
        buildDeduplicatedCountChunk<SharedKey>(*block, adaptive, local_find_state, scratch_pool, key_row_override);
    }
    else
    {
        buildBucketGroupedAggregateChunk<SharedKey>(*block, columns, adaptive, local_find_state, scratch_pool, key_row_override);
    }

    size_t batch_bytes = 0;
    if constexpr (adaptive_key_stages_bytes<SharedKey>)
        for (const auto size : adaptive.miss_key_sizes)
            batch_bytes += size;
    else
        batch_bytes = total * sizeof(SharedKey);

    if (counts_only)
        batch_bytes += total * sizeof(UInt32);
    else
        for (const auto & column : std::get<StagedChunk::AggregatePayload>(block->payload).argument_columns)
            if (column && !column->valuesHaveFixedSize())
                batch_bytes += column->byteSize();
    batch_bytes += total * (sizeof(UInt64) + (adaptive_key_stages_bytes<SharedKey> ? sizeof(UInt64) : 0));

    if (!shared.thaw_all.load(std::memory_order_relaxed))
    {
        PaddedPODArray<UInt64> sampled_hashes;
        for (const auto hash : adaptive.miss_hashes)
            if ((hash & adaptive_thaw_sample_mask) == 0)
                sampled_hashes.push_back(hash);

        std::lock_guard lock(shared.thaw_sample_mutex);
        shared.staged_records += total;
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

    adaptive.miss_source_rows.clear();
    adaptive.miss_hashes.clear();
    adaptive.miss_buckets.clear();
    adaptive.miss_key_sizes.clear();
    adaptive.miss_multiplicities.clear();

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecords, total);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecordsMerged, total - keys.size());
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedBytes, keys.key_bytes.size());

    size_t estimated_payload_bytes
        = keys.key_bytes.size() + keys.key_offsets.size() * sizeof(UInt64) + keys.routing_hashes.size() * sizeof(UInt64);
    if (const auto * counts = std::get_if<StagedChunk::CountPayload>(&block->payload))
        estimated_payload_bytes += counts->multiplicities.size() * sizeof(UInt32);
    else
        for (const auto & column : std::get<StagedChunk::AggregatePayload>(block->payload).argument_columns)
            if (column)
                estimated_payload_bytes += column->byteSize();

    stageChunk(adaptive, std::move(block), estimated_payload_bytes);
}

void Aggregator::sealValueStagedChunkDeduplicated(
    const std::vector<MutableStagedChunkPtr> & minis,
    StagedChunk & chunk) const
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;

    auto multiplicities_of = [](const StagedChunk & mini) -> const PaddedPODArray<UInt32> &
    { return std::get<StagedChunk::CountPayload>(mini.payload).multiplicities; };

    size_t total = 0;
    UInt64 total_key_bytes = 0;
    for (const auto & mini : minis)
    {
        chassert(mini->countsOnly());
        total += mini->keys.size();
        total_key_bytes += mini->keys.key_bytes.size();
    }

    auto & keys = chunk.keys;
    auto & multiplicities = chunk.payload.emplace<StagedChunk::CountPayload>().multiplicities;
    keys.fixed_key_size = minis.front()->keys.fixed_key_size;
    keys.routing_hashes.resize(total);
    multiplicities.resize(total);
    if (!keys.fixed_key_size)
        keys.key_offsets.resize(total + 1);
    keys.key_bytes.resize(total_key_bytes);

    /// The publish dedup only sees one block; keys repeating across the buffered batches are
    /// merged here, while the seal copies the records anyway. Same scheme as the publish walk:
    /// group a bucket's records by a few hash bits so a duplicate can only be one of its
    /// group's survivors, then compare within the group.
    struct StagedRef
    {
        UInt64 hash;
        UInt32 mini;
        UInt32 index;
    };
    std::vector<StagedRef> refs;
    std::vector<StagedRef> grouped;

    size_t out = 0;
    UInt64 byte_pos = 0;
    for (size_t b = 0; b < num_buckets; ++b)
    {
        keys.bucket_offsets[b] = static_cast<UInt32>(out);

        constexpr size_t num_groups = 256;
        std::array<UInt32, num_groups + 1> group_offsets{};

        /// One pass collects the bucket's records and their group histogram together; the
        /// records are then scattered whole into group order, so the dedup pass reads them
        /// sequentially instead of gathering through an index vector.
        refs.clear();
        for (size_t m = 0; m < minis.size(); ++m)
        {
            const auto & mini = *minis[m];
            for (size_t j = mini.keys.bucket_offsets[b]; j < mini.keys.bucket_offsets[b + 1]; ++j)
            {
                refs.push_back({mini.keys.routing_hashes[j], static_cast<UInt32>(m), static_cast<UInt32>(j)});
                ++group_offsets[((mini.keys.routing_hashes[j] >> 10) & 0xFF) + 1];
            }
        }
        if (refs.empty())
            continue;

        for (size_t g = 0; g < num_groups; ++g)
            group_offsets[g + 1] += group_offsets[g];
        std::array<UInt32, num_groups> group_cursor{};
        for (size_t g = 0; g < num_groups; ++g)
            group_cursor[g] = group_offsets[g];
        grouped.resize(refs.size());
        for (const auto & ref : refs)
            grouped[group_cursor[(ref.hash >> 10) & 0xFF]++] = ref;

        for (size_t g = 0; g < num_groups; ++g)
        {
            const size_t group_out_begin = out;
            for (size_t i = group_offsets[g]; i < group_offsets[g + 1]; ++i)
            {
                const auto & ref = grouped[i];
                const auto & mini = *minis[ref.mini];

                /// Batch key bytes live in the minis' padded staged arrays.
                const KeyBytesRef key{mini.keys.keyBytesAt(ref.index), ReadablePadding::AtLeast15Bytes};
                mergeOrAppendStagedCount(
                    keys, multiplicities, ref.hash, key, multiplicities_of(mini)[ref.index], group_out_begin, out, byte_pos);
            }
        }
    }

    keys.bucket_offsets[num_buckets] = static_cast<UInt32>(out);
    if (!keys.fixed_key_size)
    {
        keys.key_offsets[out] = byte_pos;
        keys.key_offsets.resize(out + 1);
    }

    keys.routing_hashes.resize(out);
    multiplicities.resize(out);
    keys.key_bytes.resize(byte_pos);
}

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

    /// The consume path's compiled aggregation applies here under the same gate: unlike the
    /// frozen consume loop, whose misses are null places the compiled row loop cannot skip,
    /// every place in a drain slice is non-null, and the staged argument columns are always
    /// dense. The sparse check only mirrors the consume-path gate - a prepared staged chunk
    /// cannot carry sparse arguments.
    bool use_compiled_functions = false;
#if USE_EMBEDDED_COMPILER
    use_compiled_functions = compiled_aggregate_functions_holder && !hasSparseArguments(prep.instructions.data());
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
    /// applies to it directly - one compiled row loop for the compiled functions, a batch pass
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
            auto replacement = createAdaptiveDrainTable(shared.early_drain_variants->type);
            auto full = std::move(shared.early_drain_variants);
            shared.early_drain_variants = std::move(replacement);
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
        drained_records += drainStagedBatch(*shared.early_drain_variants, batch, shared.cancelled, places_scratch);
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
                = drainStagedBatch(*shared.early_drain_variants, batch, shared.cancelled, places_scratch);
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
                auto replacement = createAdaptiveDrainTable(shared.early_drain_variants->type);
                detached_shared = std::move(shared.early_drain_variants);
                shared.early_drain_variants = std::move(replacement);
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

    const size_t drained_records = drainStagedBatch(*local, batch, shared.cancelled, places_scratch);
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

std::optional<Int64> Aggregator::releaseAdaptiveDrainResidue(AdaptiveAggregationSession & shared) const
{
    /// The shared table is created by the first freeze, so in a session where no producer ever
    /// froze there is none: a learning thread can stand down under memory pressure alone.
    if (!shared.initialized.load(std::memory_order_acquire))
        return {};

    std::unique_lock sweep_lock(shared.pressure_sweep_mutex);

    /// Read under the coordinator lock: the sweeps replace this pointer while holding it. They
    /// detach only at the part floor, so a residue below it is never written by them and stays
    /// resident until the merge.
    while (shared.early_drain_variants->hasData())
    {
        /// Declared before the table so that reverse-order destruction frees the table first:
        /// the budget is handed on only once the bytes it stands for are really gone.
        AdaptiveAggregationSession::SpillReservation reservation;
        AggregatedDataVariantsPtr detached;

        if (!reservation.reserveOrWait(shared, shared.early_drain_variants->allocatedBytes()))
            return {};

        auto replacement = createAdaptiveDrainTable(shared.early_drain_variants->type);
        detached = std::move(shared.early_drain_variants);
        shared.early_drain_variants = std::move(replacement);

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
    if (!quiesce.reserveOrWait(shared, adaptive_pressure_detached_bytes_budget))
        return {};

    return getCurrentQueryMemoryUsage();
}

}
