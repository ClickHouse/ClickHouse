/// The method-specialized kernels of the adaptive aggregation: the frozen consume path, the
/// staging of missed rows, and the bucket drains. They are member templates of `Aggregator`
/// (defined here rather than in `Aggregator.cpp`, following `ClientBaseOptimizedParts.cpp`),
/// dispatched over the aggregation-method variants.

#include <bit>
#include <unordered_set>

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
    extern const Event AdaptiveAggregationDrainedRecords;
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
    /// String-like keys stage their bytes: a packed reference copied as a plain value would
    /// carry a pointer into the source block, which dies when the publish compacts the
    /// arguments and releases it. The staged form of both string kinds is the raw characters,
    /// and the drain rebuilds (and for packed keys arena-persists) the table's key from them.
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
        if constexpr (requires { state.chars; state.offsets; })
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
            const UInt64 j_end = (j + 1 == out) ? byte_pos : keys.key_offsets[j + 1];
            if (j_end - keys.key_offsets[j] != size || !stagedKeyEquals(keys.key_bytes.data() + keys.key_offsets[j], key))
                continue;
            if (static_cast<UInt64>(multiplicities[j]) + multiplicity > std::numeric_limits<UInt32>::max())
                continue;
            multiplicities[j] += multiplicity;
            return;
        }

        keys.routing_hashes[out] = hash;
        multiplicities[out] = multiplicity;
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

template <typename State>
bool ALWAYS_INLINE adaptiveKeyViewsAreBlockStable(const State & state)
{
    if constexpr (requires { state.use_batch_serialize; })
        return state.use_batch_serialize;
    else
        return true;
}

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
            const UInt64 key_size = [&]() -> UInt64
            {
                if constexpr (adaptive_key_stages_bytes<typename SharedMethod::Key>)
                    return adaptiveStagedKeyBytes(key).size();
                else
                    return sizeof(typename SharedMethod::Key);
            }();
            const auto bucket = static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash));

            if (is_simple_count)
            {
                adaptive.miss_hashes.push_back(hash);
                adaptive.miss_multiplicities.push_back(static_cast<UInt32>(row_end - row_begin));
                adaptive.miss_key_sizes.push_back(key_size);
                adaptive.miss_buckets.push_back(bucket);
            }
            else
            {
                for (size_t i = row_begin; i < row_end; ++i)
                {
                    adaptive.miss_source_rows.push_back(static_cast<UInt32>(i));
                    adaptive.miss_hashes.push_back(hash);
                    adaptive.miss_buckets.push_back(bucket);
                    adaptive.miss_key_sizes.push_back(key_size);
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
                if constexpr (adaptive_key_stages_bytes<typename SharedMethod::Key>)
                    adaptive.miss_key_sizes.push_back(adaptiveStagedKeyBytes(staged_key).size());
                else
                    adaptive.miss_key_sizes.push_back(sizeof(staged_key));

                /// A serialized key view points into the reused scratch arena and can only seed
                /// the run tracking when the views are block-stable; every other key type is a
                /// self-contained value.
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

    AllocatorWithMemoryTracking<AggregateDataPtr> allocator;
    const size_t places_size = row_end;
    auto places_deleter = [&allocator, &places_size](auto * ptr)
    {
        if (ptr) [[likely]]
            allocator.deallocate(ptr, places_size);
    };
    std::unique_ptr<AggregateDataPtr[], decltype(places_deleter)> places(allocator.allocate(places_size), places_deleter);

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
                places[i] = it->getMapped();
                keyHolderDiscardKey(key_holder);
                continue;
            }
        }

        places[i] = nullptr;
        adaptive.miss_source_rows.push_back(static_cast<UInt32>(i));
        adaptive.miss_hashes.push_back(hash);
        adaptive.miss_buckets.push_back(static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash)));

        if constexpr (adaptive_key_stages_bytes<typename SharedMethod::Key>)
            adaptive.miss_key_sizes.push_back(adaptiveStagedKeyBytes(key).size());
        else
            adaptive.miss_key_sizes.push_back(sizeof(typename SharedMethod::Key));
        keyHolderDiscardKey(key_holder);
    }
    update_bypass_sampling(hits, row_end - row_begin);
    publishDelayedRecords<typename SharedMethod::Key>(columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);

    /// With no local hits every place is null and the batch pass would only skip rows; the
    /// staged records carry the block's whole contribution. Bypassed blocks are always all-miss.
    if (params.aggregates_size && hits != 0)
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
    const MutableStagedChunkPtr & block,
    AdaptiveAggregationProducer & adaptive,
    State & local_find_state,
    Arena & scratch_pool,
    std::optional<UInt32> key_row_override) const
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;
    const size_t total = adaptive.miss_hashes.size();

    /// Group (hash, record) pairs by (bucket, a few extra hash bits): a duplicate key always
    /// lands in the same group, so the dedup below only compares within a group, and group-id
    /// order is bucket-major, which is the block's slice layout. The group count scales with
    /// the batch (~16 records per group), so the histogram stays cache-resident and small
    /// batches do not pay for counters they cannot fill.
    const UInt32 sub_bits = std::min<UInt32>(8, std::bit_width(total >> 12));
    const size_t num_groups = num_buckets << sub_bits;

    auto & pairs = adaptive.sort_pairs_scratch;
    pairs.resize(total);
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
        pairs[cursor[group_of(i)]++] = {adaptive.miss_hashes[i], static_cast<UInt32>(i)};

    UInt64 total_bytes = 0;
    for (const auto size : adaptive.miss_key_sizes)
        total_bytes += size;

    auto & keys = block->keys;
    auto & multiplicities = block->payload.emplace<StagedChunk::CountPayload>().multiplicities;
    keys.routing_hashes.resize(total);
    multiplicities.resize(total);
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
            const auto [hash, idx] = pairs[i];
            const size_t size = adaptive.miss_key_sizes[idx];
            const size_t key_row = key_row_override ? *key_row_override : adaptive.miss_source_rows[idx];

            /// The key bytes are read straight from the hashing state's column when it exposes
            /// them: the generic key holder of the packed method would re-pack the key and
            /// re-compute its content hash per record, and the staged arrays already hold both.
            /// All byte uses happen inside the holder's lifetime (see `withStagedKeyBytes`).
            withStagedKeyBytes<SharedKey>(
                local_find_state,
                key_row,
                size,
                scratch_pool,
                [&](const KeyBytesRef & key)
                { mergeOrAppendStagedCount(keys, multiplicities, hash, key, adaptive.miss_multiplicities[idx], group_out_begin, out, byte_pos); });
        }
    }

    keys.bucket_offsets[num_buckets] = static_cast<UInt32>(out);
    keys.key_offsets[out] = byte_pos;

    keys.routing_hashes.resize(out);
    multiplicities.resize(out);
    keys.key_offsets.resize(out + 1);
    keys.key_bytes.resize(byte_pos);
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
    keys.key_offsets.resize_exact(total + 1);

    /// Counting sort of the staged misses by bucket: one pass over the records accumulates the
    /// record and key-byte histograms together, one pass over the buckets turns both into
    /// exclusive offsets.
    std::array<UInt32, num_buckets> cursor{};
    std::array<UInt64, num_buckets> byte_cursor{};
    for (size_t i = 0; i < total; ++i)
    {
        ++cursor[adaptive.miss_buckets[i]];
        byte_cursor[adaptive.miss_buckets[i]] += adaptive.miss_key_sizes[i];
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

        const auto size = adaptive.miss_key_sizes[i];
        const auto byte_pos = byte_cursor[b];
        byte_cursor[b] += size;
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

    /// The thaw check (see the tuning constants). The sample is collected outside the lock (a
    /// publish contributes on the order of `total` / 256 entries) and folded into the shared
    /// sampler; the verdict takes effect at every thread's next block, through the ordinary
    /// dispatch on the per-thread flags. The current records are still published: their rows
    /// were deferred by the frozen kernel and only the drain will aggregate them.
    if (!shared.thaw_all.load(std::memory_order_relaxed))
    {
        PaddedPODArray<UInt64> sampled_hashes;
        for (const auto hash : adaptive.miss_hashes)
            if ((hash & adaptive_thaw_sample_mask) == 0)
                sampled_hashes.push_back(hash);

        std::lock_guard lock(shared.thaw_sample_mutex);
        shared.staged_records += total;
        shared.thaw_sampled_records += sampled_hashes.size();
        for (const auto hash : sampled_hashes)
            shared.distinct_sampled_hashes.insert(hash);
        /// Re-checked under the lock: a thread that sampled while another was firing would
        /// otherwise fire a second time.
        if (!shared.thaw_all.load(std::memory_order_relaxed)
            && shared.staged_records >= adaptive_thaw_min_staged_records
            && shared.thaw_sampled_records > adaptive_thaw_repeat_factor * shared.distinct_sampled_hashes.size())
        {
            shared.thaw_all.store(true, std::memory_order_relaxed);
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationThaws);
            LOG_TRACE(
                log,
                "Adaptive aggregation: thawing the local tables after {} staged records (sampled repeat factor {})",
                shared.staged_records,
                shared.thaw_sampled_records / shared.distinct_sampled_hashes.size());
        }
    }

    auto block = std::make_shared<StagedChunk>();
    auto & keys = block->keys;

    if (counts_only)
    {
        buildDeduplicatedCountChunk<SharedKey>(block, adaptive, local_find_state, scratch_pool, key_row_override);
    }
    else
    {
        buildBucketGroupedAggregateChunk<SharedKey>(*block, columns, adaptive, local_find_state, scratch_pool, key_row_override);
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
        total_key_bytes += mini->keys.key_offsets[mini->keys.size()];
    }

    auto & keys = chunk.keys;
    auto & multiplicities = chunk.payload.emplace<StagedChunk::CountPayload>().multiplicities;
    keys.routing_hashes.resize(total);
    multiplicities.resize(total);
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
    std::vector<UInt32> order;

    size_t out = 0;
    UInt64 byte_pos = 0;
    for (size_t b = 0; b < num_buckets; ++b)
    {
        keys.bucket_offsets[b] = static_cast<UInt32>(out);

        refs.clear();
        for (size_t m = 0; m < minis.size(); ++m)
        {
            const auto & mini = *minis[m];
            for (size_t j = mini.keys.bucket_offsets[b]; j < mini.keys.bucket_offsets[b + 1]; ++j)
                refs.push_back({mini.keys.routing_hashes[j], static_cast<UInt32>(m), static_cast<UInt32>(j)});
        }
        if (refs.empty())
            continue;

        constexpr size_t num_groups = 256;
        std::array<UInt32, num_groups + 1> group_offsets{};
        for (const auto & ref : refs)
            ++group_offsets[((ref.hash >> 10) & 0xFF) + 1];
        for (size_t g = 0; g < num_groups; ++g)
            group_offsets[g + 1] += group_offsets[g];
        std::array<UInt32, num_groups> group_cursor{};
        for (size_t g = 0; g < num_groups; ++g)
            group_cursor[g] = group_offsets[g];
        order.resize(refs.size());
        for (size_t i = 0; i < refs.size(); ++i)
            order[group_cursor[(refs[i].hash >> 10) & 0xFF]++] = static_cast<UInt32>(i);

        for (size_t g = 0; g < num_groups; ++g)
        {
            const size_t group_out_begin = out;
            for (size_t i = group_offsets[g]; i < group_offsets[g + 1]; ++i)
            {
                const auto & ref = refs[order[i]];
                const auto & mini = *minis[ref.mini];

                /// Batch key bytes live in the minis' padded staged arrays.
                const KeyBytesRef key{mini.keys.keyBytesAt(ref.index), ReadablePadding::AtLeast15Bytes};
                mergeOrAppendStagedCount(
                    keys, multiplicities, ref.hash, key, multiplicities_of(mini)[ref.index], group_out_begin, out, byte_pos);
            }
        }
    }

    keys.bucket_offsets[num_buckets] = static_cast<UInt32>(out);
    keys.key_offsets[out] = byte_pos;

    keys.routing_hashes.resize(out);
    multiplicities.resize(out);
    keys.key_offsets.resize(out + 1);
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
        const size_t slice_begin = keys.bucket_offsets[bucket_index];
        const size_t slice_end = keys.bucket_offsets[bucket_index + 1];

        if constexpr (requires { impl.reserveAdditionalStringViewKeys(size_t{}); })
        {
            if (!reserved)
            {
                for (size_t j = slice_begin; j < slice_end; ++j)
                    if (impl.usesStringViewSubmap(keys.keyBytesAt(j)))
                        ++sampled_string_view_keys;
            }
        }

        if (const auto * counts = std::get_if<StagedChunk::CountPayload>(&block.payload))
        {
            const auto & multiplicities = counts->multiplicities;
            constexpr size_t prefetch_look_ahead = adaptive_drain_prefetch_look_ahead;
            for (size_t j = slice_begin; j < slice_end; ++j)
            {
                if (j + prefetch_look_ahead < slice_end)
                {
                    if constexpr (requires { impl.prefetchByHash(keys.routing_hashes[j]); })
                    {
                        impl.prefetchByHash(keys.routing_hashes[j + prefetch_look_ahead]);
                    }
                    else if constexpr (std::is_same_v<typename Method::Key, std::string_view>)
                    {
                        const size_t la = j + prefetch_look_ahead;
                        impl.prefetch(keys.keyBytesAt(la), keys.routing_hashes[la]);
                    }
                }

                typename Method::Data::LookupResult it;
                bool inserted = false;
                emplaceStagedKey<typename Method::Key, key_storage>(
                    impl,
                    keys.key_bytes.data() + keys.key_offsets[j],
                    keys.key_offsets[j + 1] - keys.key_offsets[j],
                    keys.routing_hashes[j],
                    *arena,
                    it,
                    inserted);

                if (inserted)
                    getInlineCountState(it->getMapped()) = multiplicities[j];
                else
                    getInlineCountState(it->getMapped()) += multiplicities[j];
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

template <AdaptiveKeyStorage key_storage, typename Method>
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
    constexpr size_t prefetch_look_ahead = adaptive_drain_prefetch_look_ahead;
    auto prefetch = [&](size_t j)
    {
        const size_t la = j + prefetch_look_ahead;
        if (la >= slice_end)
            return;
        if constexpr (requires { impl.prefetchByHash(keys.routing_hashes[j]); })
            impl.prefetchByHash(keys.routing_hashes[la]);
        else if constexpr (std::is_same_v<typename Method::Key, std::string_view>)
            impl.prefetch(keys.keyBytesAt(la), keys.routing_hashes[la]);
    };

    const auto & prep = *std::get<StagedChunk::AggregatePayload>(block.payload).prepared;

    /// `places` is indexed by absolute record index: the compacted argument columns hold record
    /// j's values at row j, so the batch calls below consume the [slice_begin, slice_end) range
    /// of the columns and of `places` directly.
    places.resize(slice_end);

    for (size_t j = slice_begin; j < slice_end; ++j)
    {
        prefetch(j);
        typename Method::Data::LookupResult it;
        bool inserted = false;
        emplaceStagedKey<typename Method::Key, key_storage>(
            impl,
            keys.key_bytes.data() + keys.key_offsets[j],
            keys.key_offsets[j + 1] - keys.key_offsets[j],
            keys.routing_hashes[j],
            *bucket_arena,
            it,
            inserted);

        AggregateDataPtr aggregate_data = nullptr;
        if (inserted)
        {
            it->getMapped() = nullptr;
            aggregate_data = bucket_arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            createAggregateStates(aggregate_data);
            it->getMapped() = aggregate_data;
        }
        else
            aggregate_data = it->getMapped();

        places[j] = aggregate_data;
    }

    if (!params.aggregates_size)
        return;

    /// Apply the aggregate functions to the delayed rows only: the slice is a contiguous row
    /// range of the compacted argument columns and of `places`, so the standard batch dispatch
    /// applies to it directly, with no gather (the staged columns are always dense, so its
    /// sparse path never fires).
    for (size_t i = 0; i < aggregate_functions.size(); ++i)
        addBatch(slice_begin, slice_end, prep.instructions.data() + i, places.data(), bucket_arena);
}

void Aggregator::drainStagedChunksEarly(AdaptiveAggregationSession & shared, AdaptiveDrainGoal goal) const
{
    /// Blocking on purpose: a producer over the memory trigger must not keep staging while the
    /// sweep sheds - pausing production here is the backpressure that makes the bound hold.
    /// Threads woken after another thread's sweep usually find memory back under the trigger
    /// and leave without work.
    std::unique_lock sweep_lock(shared.pressure_sweep_mutex);
    if (goal == AdaptiveDrainGoal::UntilLowWatermark
        && getCurrentQueryMemoryUsage() < static_cast<Int64>(params.max_bytes_before_external_group_by))
        return;

    auto chunks = shared.backlog.takeAllForPressureDrain();
    if (chunks.empty())
        return;

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPressureSweeps);

    auto & routing = *shared.early_drain_variants;
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;

    /// Bucket b's drained states live in pool b, mirroring the merge-time layout.
    while (routing.aggregates_pools.size() < num_buckets)
        routing.aggregates_pools.push_back(std::make_shared<Arena>());

    /// The sweep aims below the trigger with some headroom, so a query hovering at the
    /// threshold does not fire a sweep for every published block.
    const Int64 low_watermark = static_cast<Int64>(params.max_bytes_before_external_group_by / 4 * 3);

    PaddedPODArray<AggregateDataPtr> places_scratch;
    std::vector<StagedChunkPtr> single(1);
    size_t drained_records = 0;

    for (auto & chunk : chunks)
    {
        /// A sweep can spill gigabytes to disk; a cancelled query stops it at the next chunk
        /// (and the per-bucket drain checks the flag as well). The undrained chunks go back
        /// to the backlogs below, where they die with the session.
        if (shared.cancelled.load(std::memory_order_relaxed))
            break;

        single[0] = chunk;
        for (size_t b = 0; b < num_buckets; ++b)
        {
            const size_t records = chunk->keys.recordsForBucket(b);
            if (!records)
                continue;
            Arena * arena = routing.aggregates_pools.at(b).get();

            visitTwoLevelVariant(
                routing,
                [&](auto & method)
                {
                    drained_records += drainAdaptiveBucketBacklog<AdaptiveKeyStorage::CopyToArena>(
                        method, arena, single, b, records, places_scratch, shared.cancelled);
                });
        }
        single[0].reset();
        chunk.reset();

        if (goal == AdaptiveDrainGoal::UntilLowWatermark && getCurrentQueryMemoryUsage() < low_watermark)
            break;

        /// For a mostly-distinct key stream the transfer itself inflates memory (a staged
        /// record is slimmer than a table cell plus its states), so waiting for the end of
        /// the sweep would peak well above the trigger. Spill the routing table as soon as
        /// it holds a reasonably sized part while memory is still above the trigger; this
        /// applies to the finish-path full sweep as well, whose leftovers otherwise inflate
        /// unchecked right before the external merge.
        if (routing.size() >= adaptive_pressure_spill_min_keys
            && getCurrentQueryMemoryUsage() > static_cast<Int64>(params.max_bytes_before_external_group_by))
        {
            LOG_TRACE(
                log, "Adaptive aggregation: spilling the routing table ({} keys) under memory pressure", routing.size());
            writeToTemporaryFile(routing);
            while (routing.aggregates_pools.size() < num_buckets)
                routing.aggregates_pools.push_back(std::make_shared<Arena>());
        }
    }

    /// Chunks the watermark spared go back to the backlogs for the merge-time drain.
    for (auto & chunk : chunks)
        if (chunk)
            shared.backlog.requeue(chunk);

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPressureDrainedRecords, drained_records);
    shared.backlog.recordDrained(drained_records);
    shared.early_drain_started.store(true, std::memory_order_relaxed);
    LOG_TRACE(log, "Adaptive aggregation: pressure sweep drained {} staged records early", drained_records);

    /// A routing residue below the spill floor stays in memory on purpose: a table that small
    /// is never why memory is over the trigger, and it cannot accumulate past the floor across
    /// sweeps - the in-loop spill catches it there. It merges (or, on the external path, gets
    /// flushed at the finish) like any other variant.
}

}
