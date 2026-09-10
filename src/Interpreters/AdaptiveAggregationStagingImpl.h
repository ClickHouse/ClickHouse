#pragma once

#include <algorithm>
#include <bit>
#include <limits>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/ProfileEvents.h>
#include <Common/memcpySmall.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/AdaptiveAggregationStaging.h>
#include <base/memcmpSmall.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationSealNormalizations;
}

namespace DB::AdaptiveStagingDetail
{

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
    void ALWAYS_INLINE withStagedKeyBytes(State & state, size_t row, size_t size, Arena & scratch, Callback && callback)
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
            callback(KeyBytesRef{static_cast<std::string_view>(keyHolderGetKey(key_holder)), ReadablePadding::Exact});
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
    inline bool ALWAYS_INLINE stagedKeyEquals(const char * staged, const KeyBytesRef & key)
    {
        if (key.padding == ReadablePadding::AtLeast15Bytes && key.bytes.size() <= 64)
            return memequalSmallAllowOverflow15(staged, key.bytes.size(), key.bytes.data(), key.bytes.size());
        return memcmp(staged, key.bytes.data(), key.bytes.size()) == 0;
    }

    /// Copy candidate bytes into staged (padded) storage, honoring the source's padding. The
    /// overflow-tolerant branch also writes up to 15 bytes past the destination, so the callers
    /// must append in increasing byte order (the scribble lands in space the next append
    /// overwrites); a caller that scatters must use a plain bounded copy instead.
    inline void ALWAYS_INLINE copyStagedKeyBytes(char * staged, const KeyBytesRef & key)
    {
        if (key.bytes.empty())
            return;
        if (key.padding == ReadablePadding::AtLeast15Bytes && key.bytes.size() <= 64)
            memcpySmallAllowReadWriteOverflow15(staged, key.bytes.data(), key.bytes.size());
        else
            memcpy(staged, key.bytes.data(), key.bytes.size());
    }

    /// The count-record deduplication primitive shared by chunk building and coalescing. A duplicate key
    /// can only be one of its group's survivors, the records staged in [group_begin, out) with
    /// the same few hash bits (usually zero or one): merge the run lengths instead of staging
    /// another copy of the key, with equal hashes of distinct keys split by the byte
    /// comparison. Otherwise the record is appended at `out` and the cursors advance.
    ///
    /// The overflow-split policy lives here and only here: a survivor whose multiplicity would
    /// exceed 32 bits is skipped, because a later survivor of the same key (from a previous
    /// overflow split) may still have capacity, and otherwise the record starts a fresh
    /// survivor of the same key.
    inline void ALWAYS_INLINE mergeOrAppendStagedCount(
        StagedChunk::StagedKeys & keys,
        PaddedPODArray<UInt32> & multiplicities,
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

}

namespace DB
{

template <typename Key>
void ALWAYS_INLINE StagedChunkConverter::recordMiss(UInt32 row, UInt64 hash, UInt8 bucket, const Key & key)
{
    miss_source_rows.push_back(row);
    miss_hashes.push_back(hash);
    miss_buckets.push_back(bucket);
    if constexpr (adaptive_key_stages_bytes<Key>)
        miss_key_sizes.push_back(static_cast<std::string_view>(key).size());
}

template <typename Key>
void ALWAYS_INLINE StagedChunkConverter::recordCountRun(UInt32 row, UInt64 hash, UInt8 bucket, const Key & key, UInt32 multiplicity)
{
    recordMiss(row, hash, bucket, key);
    miss_multiplicities.push_back(multiplicity);
}

template <typename Key>
size_t StagedChunkConverter::getRecordedKeyBytes() const
{
    if constexpr (adaptive_key_stages_bytes<Key>)
    {
        size_t bytes = 0;
        for (const auto size : miss_key_sizes)
            bytes += size;
        return bytes;
    }
    else
        return miss_hashes.size() * sizeof(Key);
}

template <typename SharedKey, typename State>
void NO_INLINE StagedChunkConverter::buildCountChunk(
    StagedChunk & block,
    State & local_find_state,
    Arena & scratch_pool,
    std::optional<UInt32> key_row_override)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;
    const size_t total = miss_hashes.size();

    /// Group the records by (bucket, a few extra hash bits): a duplicate key always lands in
    /// the same group, so the dedup below only compares within a group, and group-id order is
    /// bucket-major, which is the block's slice layout. The group count scales with the batch
    /// (~16 records per group), so the histogram stays cache-resident and small batches do not
    /// pay for counters they cannot fill. A bypassed pass (see `DedupProductivity`) degrades
    /// the grouping to plain buckets and the dedup scan below to a straight append.
    const bool dedup = block_dedup.shouldDedup();
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
        const UInt32 bucket = miss_buckets[i];
        return (bucket << sub_bits) | (static_cast<UInt32>(miss_hashes[i] >> 10) & ((1u << sub_bits) - 1));
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

    /// Fixed-size keys stage no per-record size (see the kernels); conversion substitutes the
    /// compile-time constant.
    const size_t total_bytes = getRecordedKeyBytes<SharedKey>();

    auto & keys = block.keys;
    auto & multiplicities = block.payload.emplace<StagedChunk::CountPayload>().multiplicities;
    if constexpr (!adaptive_key_stages_bytes<SharedKey>)
        keys.fixed_key_size = sizeof(SharedKey);
    /// Deduplication can shrink these buffers; their initial allocation needs no growth headroom.
    keys.routing_hashes.resize_exact(total);
    multiplicities.resize_exact(total);
    if constexpr (adaptive_key_stages_bytes<SharedKey>)
        keys.key_offsets.resize_exact(total + 1);
    keys.key_bytes.resize_exact(total_bytes);

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
            const UInt64 hash = miss_hashes[idx];
            const size_t size = [&]
            {
                if constexpr (adaptive_key_stages_bytes<SharedKey>)
                    return miss_key_sizes[idx];
                else
                    return sizeof(SharedKey);
            }();
            const size_t key_row = key_row_override ? *key_row_override : miss_source_rows[idx];

            /// The key bytes are read straight from the hashing state's column when it exposes
            /// them: the generic key holder of the packed method would re-pack the key and
            /// re-compute its content hash per record, and the staged arrays already hold both.
            /// All byte uses happen inside the holder's lifetime (see `withStagedKeyBytes`).
            /// A bypassed pass hands the append an empty candidate range, so nothing is scanned.
            AdaptiveStagingDetail::withStagedKeyBytes<SharedKey>(
                local_find_state,
                key_row,
                size,
                scratch_pool,
                [&](const AdaptiveStagingDetail::KeyBytesRef & key)
                {
                    AdaptiveStagingDetail::mergeOrAppendStagedCount(
                        keys, multiplicities, hash, key, miss_multiplicities[idx], dedup ? group_out_begin : out, out, byte_pos);
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
        block_dedup.record(total, out);
}

template <typename SharedKey, typename State>
void NO_INLINE StagedChunkConverter::buildAggregateChunk(
    StagedChunk & block,
    const Columns & columns,
    const ColumnNumbersList & aggregates_positions,
    State & local_find_state,
    Arena & scratch_pool,
    std::optional<UInt32> key_row_override)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;
    const size_t total = miss_hashes.size();
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
    /// Fixed-size keys stage no per-record size (see the kernels); conversion substitutes the
    /// compile-time constant.
    const auto staged_key_size = [&](size_t record)
    {
        if constexpr (adaptive_key_stages_bytes<SharedKey>)
            return miss_key_sizes[record];
        else
            return sizeof(SharedKey);
    };

    std::array<UInt32, num_buckets> cursor{};
    std::array<UInt64, num_buckets> byte_cursor{};
    for (size_t i = 0; i < total; ++i)
    {
        ++cursor[miss_buckets[i]];
        byte_cursor[miss_buckets[i]] += staged_key_size(i);
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
    if (!aggregates_positions.empty())
    {
        gather_indexes = ColumnUInt32::create();
        gather_indexes->getData().resize_exact(total);
        gather_data = gather_indexes->getData().data();
    }

    for (size_t i = 0; i < total; ++i)
    {
        const auto b = miss_buckets[i];
        const auto pos = cursor[b]++;
        keys.routing_hashes[pos] = miss_hashes[i];

        if (gather_data)
            gather_data[pos] = miss_source_rows[i];

        const auto size = staged_key_size(i);
        const auto byte_pos = byte_cursor[b];
        byte_cursor[b] += size;
        if constexpr (adaptive_key_stages_bytes<SharedKey>)
            keys.key_offsets[pos] = byte_pos;

        /// The same byte extraction the count path uses: states that expose their padded
        /// column buffers hand the bytes out directly (in particular, the packed-string method
        /// does not rebuild the key, which would re-hash its content per record). The copy is
        /// a bounded `memcpy`: records scatter into bucket-grouped positions, so an
        /// overflow-tolerant write could overwrite neighbors already in place. Empty packed
        /// keys have a null data pointer, which `memcpy` does not accept.
        const size_t key_row = key_row_override ? *key_row_override : miss_source_rows[i];
        AdaptiveStagingDetail::withStagedKeyBytes<SharedKey>(
            local_find_state,
            key_row,
            size,
            scratch_pool,
            [&](const AdaptiveStagingDetail::KeyBytesRef & key)
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
            /// accounting measure the real payload, instruction preparation wires the
            /// columns directly instead of pinning a second, dense copy next to the wrapper,
            /// and gathered `LowCardinality` values own their storage independently of the
            /// source block's dictionary.
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
MutableStagedChunkPtr StagedChunkConverter::build(
    const Columns & columns,
    const ColumnNumbersList & aggregates_positions,
    State & local_find_state,
    Arena & scratch_pool,
    bool counts_only,
    std::optional<UInt32> key_row_override)
{
    const size_t records = miss_hashes.size();
    chassert(miss_source_rows.size() == records && miss_buckets.size() == records);
    chassert(miss_key_sizes.size() == (adaptive_key_stages_bytes<SharedKey> ? records : 0));
    chassert(miss_multiplicities.size() == (counts_only ? records : 0));
    auto chunk = std::make_shared<StagedChunk>();
    if (counts_only)
        buildCountChunk<SharedKey>(*chunk, local_find_state, scratch_pool, key_row_override);
    else
        buildAggregateChunk<SharedKey>(
            *chunk, columns, aggregates_positions, local_find_state, scratch_pool, key_row_override);
    return chunk;
}

}
