#include <algorithm>
#include <bit>
#include <limits>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/memcpySmall.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/AdaptiveAggregationChunkInfo.h>
#include <Interpreters/AdaptiveAggregationStaging.h>
#include <Processors/Merges/Algorithms/PartitionedChunkCoalescing.h>
#include <base/memcmpSmall.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationStagedRecordsMerged;
    extern const Event AdaptiveAggregationSealedChunks;
    extern const Event AdaptiveAggregationSealNormalizations;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Both key views refer to padded miss recordings or staged storage, so small comparisons may
/// read up to 15 bytes past either end.
bool ALWAYS_INLINE stagedKeyEquals(const char * staged, std::string_view key)
{
    if (key.size() <= 64)
        return memequalSmallAllowOverflow15(staged, key.size(), key.data(), key.size());
    return memcmp(staged, key.data(), key.size()) == 0;
}

/// Appends from padded storage in increasing byte order. The small-copy primitive may overwrite
/// up to 15 bytes after the destination; the next append replaces that padding.
void ALWAYS_INLINE copyStagedKeyBytes(char * staged, std::string_view key)
{
    if (key.empty())
        return;
    if (key.size() <= 64)
        memcpySmallAllowReadWriteOverflow15(staged, key.data(), key.size());
    else
        memcpy(staged, key.data(), key.size());
}

/// Equal keys share a hash group. Fold into a surviving count if its multiplicity fits; otherwise
/// keep searching for another survivor with capacity, or append a new record.
void ALWAYS_INLINE mergeOrAppendStagedCount(
    StagedChunk::StagedKeys & keys,
    PaddedPODArray<UInt32> & multiplicities,
    UInt64 hash,
    std::string_view key,
    UInt32 multiplicity,
    size_t group_begin,
    size_t & out,
    UInt64 & byte_pos)
{
    const size_t size = key.size();
    for (size_t j = group_begin; j < out; ++j)
    {
        if (keys.routing_hashes[j] != hash)
            continue;
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

size_t StagedChunk::byteSize() const
{
    size_t bytes = keys.byteSize();
    if (const auto * counts = std::get_if<CountPayload>(&payload))
        bytes += counts->multiplicities.size() * sizeof(UInt32);
    else
        for (const auto & column : std::get<AggregatePayload>(payload).argument_columns)
            bytes += column->byteSize();
    return bytes;
}

size_t StagedChunk::allocatedBytes() const
{
    size_t bytes = keys.key_bytes.allocated_bytes() + keys.key_offsets.allocated_bytes() + keys.routing_hashes.allocated_bytes();
    if (const auto * counts = std::get_if<CountPayload>(&payload))
        bytes += counts->multiplicities.allocated_bytes();
    else
        for (const auto & column : std::get<AggregatePayload>(payload).argument_columns)
            bytes += column->allocatedBytes();
    return bytes;
}

MutableStagedChunkPtr StagedChunk::cut(size_t start, size_t length) const
{
    chassert(start <= keys.size() && length <= keys.size() - start);
    const auto & source_keys = keys;
    const size_t end = start + length;

    /// Slices have their final size. Exact reservations avoid growth headroom that would
    /// inflate their allocation-based pressure estimates beyond the chosen range size.
    auto piece = std::make_shared<StagedChunk>();
    auto & result_keys = piece->keys;
    result_keys.fixed_key_size = source_keys.fixed_key_size;
    result_keys.routing_hashes.reserve_exact(length);
    result_keys.routing_hashes.insert(source_keys.routing_hashes.begin() + start, source_keys.routing_hashes.begin() + end);

    const size_t byte_begin = source_keys.keyByteOffsetAt(start);
    const size_t byte_end = source_keys.keyByteOffsetAt(end);
    result_keys.key_bytes.reserve_exact(byte_end - byte_begin);
    result_keys.key_bytes.insert(source_keys.key_bytes.begin() + byte_begin, source_keys.key_bytes.begin() + byte_end);
    if (!source_keys.fixed_key_size)
    {
        result_keys.key_offsets.reserve_exact(length + 1);
        for (size_t i = start; i <= end; ++i)
            result_keys.key_offsets.push_back(source_keys.key_offsets[i] - byte_begin);
    }

    /// Clamping preserves intersections with each bucket, including cuts inside a bucket.
    for (size_t b = 0; b <= ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++b)
        result_keys.bucket_offsets[b] = static_cast<UInt32>(std::clamp<size_t>(source_keys.bucket_offsets[b], start, end) - start);

    if (const auto * counts = std::get_if<CountPayload>(&payload))
    {
        auto & multiplicities = piece->payload.emplace<CountPayload>().multiplicities;
        multiplicities.reserve_exact(length);
        multiplicities.insert(counts->multiplicities.begin() + start, counts->multiplicities.begin() + end);
    }
    else
    {
        const auto & columns = std::get<AggregatePayload>(payload).argument_columns;
        auto & argument_columns = piece->payload.emplace<AggregatePayload>().argument_columns;
        argument_columns.reserve(columns.size());
        for (const auto & column : columns)
            argument_columns.push_back(column->cut(start, length));
    }
    return piece;
}

bool StagedChunk::isWellFormed() const
{
    const size_t records = keys.size();
    if (keys.bucket_offsets.front() != 0 || keys.bucket_offsets.back() != records
        || !std::is_sorted(keys.bucket_offsets.begin(), keys.bucket_offsets.end()))
        return false;
    if (keys.fixed_key_size)
    {
        if (!keys.key_offsets.empty() || keys.key_bytes.size() != records * keys.fixed_key_size)
            return false;
    }
    else
    {
        if (keys.key_offsets.size() != records + 1 || keys.key_offsets.front() != 0
            || keys.key_offsets.back() != keys.key_bytes.size()
            || !std::is_sorted(keys.key_offsets.begin(), keys.key_offsets.end()))
            return false;
    }
    if (const auto * counts = std::get_if<CountPayload>(&payload))
        return counts->multiplicities.size() == records;
    for (const auto & column : std::get<AggregatePayload>(payload).argument_columns)
        if (!column || column->size() != records)
            return false;
    return true;
}

namespace
{

/// Where the chunk build reads a record's key bytes. Every source is a padded container (the
/// recording's `PaddedPODArray`s or a `ColumnString`'s chars), so the overflow-tolerant small
/// compare and copy primitives are legal on every key. A fixed size is a compile-time constant,
/// so an integer key compares and copies as one word.
template <size_t N>
struct FixedKeyLayout
{
    static constexpr size_t fixed_size = N;
    const char * bytes;

    std::string_view at(size_t record) const { return {bytes + record * N, N}; }
    size_t totalBytes(size_t records) const { return records * N; }
};

struct RecordedKeyLayout
{
    static constexpr size_t fixed_size = 0;
    const char * bytes;
    const UInt64 * offsets;

    std::string_view at(size_t record) const { return {bytes + offsets[record], offsets[record + 1] - offsets[record]}; }
    size_t totalBytes(size_t records) const { return offsets[records]; }
};

/// The key column's bytes indexed by the record's source row; the recording holds the sizes. A
/// constant key's column has one row, which every record reads.
template <bool constant_key>
struct ColumnKeyLayout
{
    static constexpr size_t fixed_size = 0;
    const char * chars;
    const IColumn::Offset * offsets;
    const UInt32 * rows;
    const UInt64 * sizes;

    std::string_view at(size_t record) const
    {
        const ssize_t row = constant_key ? 0 : static_cast<ssize_t>(rows[record]);
        return {chars + offsets[row - 1], sizes[record]};
    }
    size_t totalBytes(size_t records) const
    {
        size_t bytes = 0;
        for (size_t record = 0; record < records; ++record)
            bytes += sizes[record];
        return bytes;
    }
};

}

Chunk StagedChunkConverter::build(
    std::span<const ColumnPtr> arguments, const IColumn * key_column, const AdaptiveAggregationMissesInfo & misses, bool counts_only)
{
    using KeyBytes = AdaptiveAggregationMissesInfo::KeyBytes;
    const size_t records = misses.size();
    chassert(records != 0 && misses.source_rows.size() == records && misses.buckets.size() == records);
    chassert(misses.multiplicities.size() == (counts_only ? records : 0));
    chassert(std::is_sorted(misses.source_rows.begin(), misses.source_rows.end()));
    chassert(key_column || misses.key_bytes_source != KeyBytes::InKeyColumn);

    auto info = std::make_shared<StagedKeysInfo>(misses.use_own_memory_tracker);
    auto & keys = info->keys;
    keys.fixed_key_size = misses.fixed_key_size;

    const auto build_with = [&](const auto & layout) -> Columns
    {
        if (counts_only)
            return {buildCountColumn(keys, misses, layout)};
        return buildAggregateColumns(keys, arguments, misses, layout);
    };

    Columns columns;
    switch (misses.key_bytes_source)
    {
        case KeyBytes::Fixed:
        {
            const char * bytes = misses.key_bytes.data();
            switch (misses.fixed_key_size)
            {
                case 1: columns = build_with(FixedKeyLayout<1>{bytes}); break;
                case 2: columns = build_with(FixedKeyLayout<2>{bytes}); break;
                case 4: columns = build_with(FixedKeyLayout<4>{bytes}); break;
                case 8: columns = build_with(FixedKeyLayout<8>{bytes}); break;
                case 16: columns = build_with(FixedKeyLayout<16>{bytes}); break;
                case 32: columns = build_with(FixedKeyLayout<32>{bytes}); break;
                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Adaptive aggregation staged a fixed key of {} bytes", misses.fixed_key_size);
            }
            break;
        }
        case KeyBytes::Recorded:
            columns = build_with(RecordedKeyLayout{misses.key_bytes.data(), misses.key_offsets.data()});
            break;
        case KeyBytes::InKeyColumn:
        {
            const auto & column = assert_cast<const ColumnString &>(
                misses.constant_key ? assert_cast<const ColumnConst &>(*key_column).getDataColumn() : *key_column);
            const auto * chars = reinterpret_cast<const char *>(column.getChars().data());
            const auto * offsets = column.getOffsets().data();
            if (misses.constant_key)
                columns = build_with(ColumnKeyLayout<true>{chars, offsets, misses.source_rows.data(), misses.key_sizes.data()});
            else
                columns = build_with(ColumnKeyLayout<false>{chars, offsets, misses.source_rows.data(), misses.key_sizes.data()});
            break;
        }
    }

    Chunk result(std::move(columns), keys.size());
    result.getChunkInfos().add(std::move(info));
    return result;
}

template <typename KeyLayout>
ColumnPtr StagedChunkConverter::buildCountColumn(
    StagedChunk::StagedKeys & keys, const AdaptiveAggregationMissesInfo & misses, const KeyLayout & layout)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;
    const size_t total = misses.size();

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

    const size_t total_bytes = layout.totalBytes(total);
    recorded_key_bytes = total_bytes;

    auto column = ColumnUInt32::create();
    auto & multiplicities = column->getData();
    /// Deduplication can shrink these buffers; their initial allocation needs no growth headroom.
    keys.routing_hashes.resize_exact(total);
    multiplicities.resize_exact(total);
    if constexpr (!KeyLayout::fixed_size)
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

        /// A bypassed pass hands the append an empty candidate range, so nothing is scanned.
        const size_t group_out_begin = out;
        for (size_t i = group_begin; i < group_end; ++i)
        {
            const auto idx = grouped_indexes[i];
            mergeOrAppendStagedCount(
                keys, multiplicities, misses.hashes[idx], layout.at(idx), misses.multiplicities[idx],
                dedup ? group_out_begin : out, out, byte_pos);
        }
    }

    keys.bucket_offsets[num_buckets] = static_cast<UInt32>(out);
    if constexpr (!KeyLayout::fixed_size)
    {
        keys.key_offsets[out] = byte_pos;
        keys.key_offsets.resize(out + 1);
    }

    keys.routing_hashes.resize(out);
    multiplicities.resize(out);
    keys.key_bytes.resize(byte_pos);

    if (dedup)
        block_dedup.record(total, out);
    return column;
}

template <typename KeyLayout>
Columns StagedChunkConverter::buildAggregateColumns(
    StagedChunk::StagedKeys & keys,
    std::span<const ColumnPtr> arguments,
    const AdaptiveAggregationMissesInfo & misses,
    const KeyLayout & layout)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;
    const size_t total = misses.size();

    /// The sizes are exact and final, and the chunk can sit on a backlog for the rest of the
    /// query, so the arrays are sized without the power-of-two growth headroom.
    keys.routing_hashes.resize_exact(total);
    if constexpr (!KeyLayout::fixed_size)
        keys.key_offsets.resize_exact(total + 1);

    /// Counting sort of the recorded misses by bucket: one pass over the records accumulates the
    /// record and key-byte histograms together, one pass over the buckets turns both into
    /// exclusive offsets.
    std::array<UInt32, num_buckets> cursor{};
    std::array<UInt64, num_buckets> byte_cursor{};
    for (size_t i = 0; i < total; ++i)
    {
        ++cursor[misses.buckets[i]];
        byte_cursor[misses.buckets[i]] += layout.at(i).size();
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
    if constexpr (!KeyLayout::fixed_size)
        keys.key_offsets[total] = byte_offset;

    keys.key_bytes.resize_exact(byte_offset);
    recorded_key_bytes = byte_offset;

    /// The records' source row numbers in bucket-grouped order: the gather indexes that compact
    /// the argument columns below. A zero-aggregate block stages keys only, so it needs none.
    ColumnUInt32::MutablePtr gather_indexes;
    UInt32 * gather_data = nullptr;
    if (!arguments.empty())
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

        const auto key = layout.at(i);
        const auto byte_pos = byte_cursor[b];
        byte_cursor[b] += key.size();
        if constexpr (!KeyLayout::fixed_size)
            keys.key_offsets[pos] = byte_pos;

        /// The copy is a bounded `memcpy`: records scatter into bucket-grouped positions, so an
        /// overflow-tolerant write could overwrite neighbors already in place. Empty packed
        /// keys have a null data pointer, which `memcpy` does not accept.
        if (!key.empty())
            memcpy(keys.key_bytes.data() + byte_pos, key.data(), key.size());
    }

    Columns gathered_arguments;
    gathered_arguments.reserve(arguments.size());
    for (const auto & column : arguments)
    {
        /// The gather stays on the cheap representation: a constant is resized and a sparse
        /// column is gathered in its sparse form, instead of materializing the whole block just
        /// to gather the staged subset from it. The gathered column is then normalized to the
        /// dense form the drain consumes (the representation wrappers stripped recursively, then
        /// `LowCardinality`), so the chunk stores exactly what will be drained: the thaw estimate
        /// and the pinned memory accounting measure the real payload, instruction preparation
        /// wires the columns directly, and gathered `LowCardinality` values own their storage
        /// independently of the source block's dictionary.
        ColumnPtr gathered = isColumnConst(*column) ? column->cloneResized(total) : column->index(*gather_indexes, 0);
        ColumnPtr normalized = recursiveRemoveLowCardinality(gathered->convertToFullIfWrapped());
        if (normalized.get() != gathered.get())
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSealNormalizations);
        gathered_arguments.push_back(std::move(normalized));
    }
    return gathered_arguments;
}

namespace
{

const StagedChunk::StagedKeys & stagedKeys(const Chunk & chunk)
{
    const auto info = chunk.getChunkInfos().get<StagedKeysInfo>();
    chassert(info);
    return info->keys;
}

/// Borrows each chunk's layout once, outside the per-bucket and per-record loops.
struct StagedChunkView
{
    const StagedChunk::StagedKeys & keys;
    const Columns & columns;
};

auto stagedOffsets(std::span<const StagedChunkView> minis)
{
    return [minis](size_t source, size_t partition) { return minis[source].keys.bucket_offsets[partition]; };
}

/// Concatenates the minis' bucket-grouped keys into `keys`: bucket b's records are the
/// concatenation of the minis' b-slices in buffer order. A caller's payload concatenation must
/// walk the same (bucket, mini) order, so a record keeps one position across the key, hash,
/// and payload arrays.
void concatenateStagedKeys(StagedChunk::StagedKeys & keys, std::span<const StagedChunkView> minis)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;

    size_t total = 0;
    for (size_t b = 0; b < num_buckets; ++b)
    {
        keys.bucket_offsets[b] = static_cast<UInt32>(total);
        for (const auto & mini : minis)
            total += mini.keys.recordsForBucket(b);
    }
    keys.bucket_offsets[num_buckets] = static_cast<UInt32>(total);

    UInt64 total_key_bytes = 0;
    for (const auto & mini : minis)
        total_key_bytes += mini.keys.key_bytes.size();

    keys.routing_hashes.resize_exact(total);
    forEachPartitionedChunkRange(
        num_buckets, minis.size(), stagedOffsets(minis),
        [&](size_t source, size_t begin, size_t length, size_t destination)
        {
            memcpy(&keys.routing_hashes[destination], &minis[source].keys.routing_hashes[begin], length * sizeof(UInt64));
        });

    keys.fixed_key_size = minis.front().keys.fixed_key_size;
    if (!keys.fixed_key_size)
        keys.key_offsets.resize_exact(total + 1);
    keys.key_bytes.resize_exact(total_key_bytes);
    {
        UInt64 byte_pos = 0;
        forEachPartitionedChunkRange(
            num_buckets, minis.size(), stagedOffsets(minis),
            [&](size_t source, size_t begin, size_t length, size_t destination)
            {
                const auto & mini_keys = minis[source].keys;
                const UInt64 src_begin = mini_keys.keyByteOffsetAt(begin);
                const UInt64 slice_bytes = mini_keys.keyByteOffsetAt(begin + length) - src_begin;
                memcpy(keys.key_bytes.data() + byte_pos, mini_keys.key_bytes.data() + src_begin, slice_bytes);
                if (!keys.fixed_key_size)
                    for (size_t j = 0; j < length; ++j)
                        keys.key_offsets[destination + j] = byte_pos + (mini_keys.key_offsets[begin + j] - src_begin);
                byte_pos += slice_bytes;
            });
        if (!keys.fixed_key_size)
            keys.key_offsets[total] = byte_pos;
    }
}

ColumnPtr coalesceCountChunksWithDeduplication(std::span<const StagedChunkView> minis, StagedChunk::StagedKeys & keys)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;

    auto multiplicities_of = [](const StagedChunkView & mini) -> const PaddedPODArray<UInt32> &
    {
        return assert_cast<const ColumnUInt32 &>(*mini.columns.front()).getData();
    };

    size_t total = 0;
    UInt64 total_key_bytes = 0;
    for (const auto & mini : minis)
    {
        total += mini.keys.size();
        total_key_bytes += mini.keys.key_bytes.size();
    }

    auto column = ColumnUInt32::create();
    auto & multiplicities = column->getData();
    keys.fixed_key_size = minis.front().keys.fixed_key_size;
    keys.routing_hashes.resize_exact(total);
    multiplicities.resize_exact(total);
    if (!keys.fixed_key_size)
        keys.key_offsets.resize_exact(total + 1);
    keys.key_bytes.resize_exact(total_key_bytes);

    /// Group each bucket's records by a few hash bits before comparing keys. Repeated keys
    /// fall in the same group, so deduplication only scans that group's surviving records.
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
            const auto & mini_keys = minis[m].keys;
            for (size_t j = mini_keys.bucket_offsets[b]; j < mini_keys.bucket_offsets[b + 1]; ++j)
            {
                refs.push_back({mini_keys.routing_hashes[j], static_cast<UInt32>(m), static_cast<UInt32>(j)});
                ++group_offsets[((mini_keys.routing_hashes[j] >> 10) & 0xFF) + 1];
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
                const auto & mini = minis[ref.mini];

                mergeOrAppendStagedCount(
                    keys, multiplicities, ref.hash, mini.keys.keyBytesAt(ref.index),
                    multiplicities_of(mini)[ref.index], group_out_begin, out, byte_pos);
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
    return column;
}

}

Chunk StagedChunkCoalescer::add(Chunk chunk)
{
    const size_t chunk_bytes = chunk.bytes() + stagedKeys(chunk).byteSize();
    /// Large candidates gain little from another full copy; buffered smaller candidates remain pending.
    if (chunk_bytes * 2 >= adaptive_coalescing_target_bytes)
        return chunk;

    pending_chunks.push_back(std::move(chunk));
    pending_staged_bytes += chunk_bytes;
    if (pending_staged_bytes >= adaptive_coalescing_target_bytes)
        return flush();
    return {};
}

Chunk StagedChunkCoalescer::flush()
{
    if (pending_chunks.empty())
        return {};

    auto & minis = pending_chunks;
    const size_t num_minis = minis.size();
    if (num_minis == 1)
    {
        auto chunk = std::move(minis.front());
        minis.clear();
        pending_staged_bytes = 0;
        return chunk;
    }

    auto info = std::make_shared<StagedKeysInfo>(false);
    auto & keys = info->keys;
    Columns columns;
    std::vector<StagedChunkView> sources;
    sources.reserve(num_minis);
    size_t input_records = 0;
    for (const auto & mini : minis)
    {
        sources.push_back({stagedKeys(mini), mini.getColumns()});
        input_records += mini.getNumRows();
        chassert(sources.back().columns.size() == sources.front().columns.size());
        chassert(sources.back().keys.fixed_key_size == sources.front().keys.fixed_key_size);
    }

    if (counts_only && coalescing_dedup.shouldDedup())
    {
        columns.push_back(coalesceCountChunksWithDeduplication(sources, keys));
        coalescing_dedup.record(input_records, keys.size());
    }
    else
    {
        concatenateStagedKeys(keys, sources);
        columns.reserve(minis.front().getNumColumns());
        for (size_t position = 0; position < minis.front().getNumColumns(); ++position)
        {
            VectorWithMemoryTracking<ColumnPtr> column_sources;
            column_sources.reserve(num_minis);
            for (const auto & source : sources)
                column_sources.push_back(source.columns[position]);
            columns.push_back(coalescePartitionedColumn(
                column_sources, ADAPTIVE_AGGREGATION_NUM_BUCKETS, stagedOffsets(sources)));
        }
    }

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSealedChunks);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecordsMerged, input_records - keys.size());
    static const auto log = getLogger("StagedChunkCoalescer");
    LOG_TRACE(log, "Adaptive aggregation: coalesced {} staged batches into one chunk of {} records", num_minis, keys.size());

    Chunk result(std::move(columns), keys.size());
    result.getChunkInfos().add(std::move(info));
    minis.clear();
    pending_staged_bytes = 0;
    return result;
}

}
