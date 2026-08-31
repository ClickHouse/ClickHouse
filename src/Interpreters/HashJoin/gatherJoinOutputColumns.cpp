#include <Interpreters/HashJoin/gatherJoinOutputColumns.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/memcpySmall.h>
#include <Common/typeid_cast.h>

#include <cstring>

namespace ProfileEvents
{
    extern const Event HashJoinDirectGatheredValues;
}

namespace DB
{

namespace
{

/// At a few nanoseconds of loop body per row, 32 rows of lead cover one source row's memory latency.
constexpr size_t look_ahead = 32;

/// `STRIDE` is 0 when the width is only known at run time, which is what covers `FixedString(n)` for
/// an arbitrary `n`; a compile-time width turns the copy into a single load and store. `default_byte`
/// is what a zero ref word writes into every byte of the value: 0 for a value plane and 1 for a
/// `Nullable` null map, whose default is NULL.
template <bool from_row_list, size_t STRIDE>
void gatherFixedStride(
    IColumn & dst,
    const void * const * sources,
    size_t dynamic_stride,
    const UInt64 * row_refs_begin,
    const UInt64 * row_refs_end,
    size_t rows_to_add,
    UInt8 default_byte)
{
    const size_t stride = STRIDE ? STRIDE : dynamic_stride;
    const std::span<char> out_span = dst.insertRawUninitialized(rows_to_add);
    chassert(out_span.size() == rows_to_add * stride);

    char * out = out_span.data();

    auto copy_ref = [&](UInt64 ref_word)
    {
        const char * from = static_cast<const char *>(sources[refWordBlockNo(ref_word)])
            + static_cast<size_t>(refWordRowNo(ref_word)) * stride;
        memcpy(out, from, stride);
        out += stride;
    };

    const size_t num_refs = row_refs_end - row_refs_begin;
    for (size_t i = 0; i < num_refs; ++i)
    {
        if (i + look_ahead < num_refs)
        {
            /// Only an inline word carries a (block_no, row_number) address.
            const UInt64 ahead = row_refs_begin[i + look_ahead];
            if (refWordIsInline(ahead))
                __builtin_prefetch(
                    static_cast<const char *>(sources[refWordBlockNo(ahead)])
                    + static_cast<size_t>(refWordRowNo(ahead)) * stride);
        }

        const UInt64 word = row_refs_begin[i];
        if (!word)
        {
            memset(out, default_byte, stride);
            out += stride;
            continue;
        }

        if constexpr (from_row_list)
        {
            for (const UInt64 ref_word : refsOf(word))
                copy_ref(ref_word);
        }
        else
        {
            chassert(refWordIsInline(word));
            copy_ref(word);
        }
    }

    chassert(out == out_span.data() + out_span.size());
}

template <bool from_row_list>
void gatherFixedDispatch(
    IColumn & dst,
    const void * const * sources,
    size_t stride,
    const UInt64 * row_refs_begin,
    const UInt64 * row_refs_end,
    size_t rows_to_add,
    UInt8 default_byte)
{
    switch (stride)
    {
#define M(STRIDE) \
    case (STRIDE): \
        gatherFixedStride<from_row_list, (STRIDE)>(dst, sources, stride, row_refs_begin, row_refs_end, rows_to_add, default_byte); \
        break;
        M(1)
        M(2)
        M(4)
        M(8)
        M(16)
        M(32)
#undef M
        default: gatherFixedStride<from_row_list, 0>(dst, sources, stride, row_refs_begin, row_refs_end, rows_to_add, default_byte); break;
    }
}

/// Remap one inline word's row through a replicated block's indexes: `row' = indexes[row]` addresses
/// the nested column. An identity entry (a block that stores the column plainly) passes the word through.
ALWAYS_INLINE UInt64 remapFlatWord(UInt64 word, const DirectGatherRowRemap * remap_by_block)
{
    const DirectGatherRowRemap & remap = remap_by_block[refWordBlockNo(word)];
    if (!remap.indexes_data)
        return word;
    const size_t row = refWordRowNo(word);
    size_t mapped;
    switch (remap.index_width)
    {
        case 1: mapped = static_cast<const UInt8 *>(remap.indexes_data)[row]; break;
        case 2: mapped = static_cast<const UInt16 *>(remap.indexes_data)[row]; break;
        case 4: mapped = static_cast<const UInt32 *>(remap.indexes_data)[row]; break;
        default: mapped = static_cast<const UInt64 *>(remap.indexes_data)[row]; break;
    }
    /// The nested column of a replicated column is never larger than the block it deduplicates,
    /// which addBlockToJoin limits to 4G rows.
    chassert(mapped <= std::numeric_limits<UInt32>::max());
    return (word & 0xFFFFFFFF00000000ull) | static_cast<UInt32>(mapped);
}

/// A "flat" ref sequence has one word per output row: 0 is a default row, anything else an inline
/// (block_no, row_no) word. `buildOutputFromBlocks<from_row_list = false>` and the limit-and-offset
/// walk hand exactly this shape already, so without a remap they pass through; a row-list sequence
/// is expanded into `storage`, and a replicated source has its rows remapped at the same time.
template <bool from_row_list>
const UInt64 * normalizeRefWords(
    const UInt64 * row_refs_begin,
    const UInt64 * row_refs_end,
    const DirectGatherRowRemap * remap_by_block,
    size_t rows_to_add,
    PaddedPODArray<UInt64> & storage)
{
    if constexpr (!from_row_list)
    {
        if (!remap_by_block)
            return row_refs_begin;
    }

    storage.resize(rows_to_add);
    UInt64 * out = storage.data();
    for (const UInt64 * word_i = row_refs_begin; word_i != row_refs_end; ++word_i)
    {
        const UInt64 word = *word_i;
        if (!word)
        {
            *out++ = 0;
            continue;
        }
        if constexpr (from_row_list)
        {
            for (const UInt64 ref_word : refsOf(word))
                *out++ = remap_by_block ? remapFlatWord(ref_word, remap_by_block) : ref_word;
        }
        else
        {
            chassert(refWordIsInline(word));
            *out++ = remapFlatWord(word, remap_by_block);
        }
    }
    chassert(out == storage.data() + rows_to_add);
    return storage.data();
}

/// A contiguous run of source rows in one block, used below an `Array` node: the nested rows of
/// consecutive array values are themselves consecutive, so a child is gathered by ranges instead of
/// row by row. Ranges never carry defaults - an unmatched row contributes an empty array.
struct GatherRange
{
    UInt32 block_no = 0;
    UInt64 begin = 0;
    UInt64 length = 0;
};

using GatherRanges = PaddedPODArray<GatherRange>;

/// Append a run to `ranges`, extending the last range when the run adjoins it: consecutive source
/// rows of one block (a row-list batch, a rerange) then become a single nested copy.
void appendGatherRange(GatherRanges & ranges, UInt32 block_no, UInt64 begin, UInt64 length)
{
    if (!ranges.empty() && ranges.back().block_no == block_no && ranges.back().begin + ranges.back().length == begin)
        ranges.back().length += length;
    else
        ranges.push_back(GatherRange{.block_no = block_no, .begin = begin, .length = length});
}

/// Rebase one range's source offsets onto the destination, whose last offset is `cursor`; returns the
/// contiguous run of the nested plane (chars, array elements) that the range covers.
GatherRange rebaseOffsets(const UInt64 * offsets, const GatherRange & range, UInt64 *& out_offsets, UInt64 cursor)
{
    const UInt64 base = offsets[static_cast<ssize_t>(range.begin) - 1];
    const UInt64 rebase = cursor - base;
    for (UInt64 row = range.begin; row < range.begin + range.length; ++row)
        *out_offsets++ = offsets[row] + rebase;
    return {.block_no = range.block_no, .begin = base, .length = offsets[range.begin + range.length - 1] - base};
}

/// One bulk copy per range from a per-block plane of `stride`-byte values into `dst`'s raw buffer.
void gatherRawRanges(IColumn & dst, const void * const * bases, size_t stride, const GatherRanges & ranges, size_t total_rows)
{
    const std::span<char> out_span = dst.insertRawUninitialized(total_rows);
    chassert(out_span.size() == total_rows * stride);
    char * out = out_span.data();
    for (const GatherRange & range : ranges)
    {
        const size_t bytes = range.length * stride;
        memcpy(out, static_cast<const char *>(bases[range.block_no]) + range.begin * stride, bytes);
        out += bytes;
    }
    chassert(out == out_span.data() + out_span.size());
}

void gatherNodeRows(IColumn & dst, const DirectGatherNode & node, const UInt64 * words, size_t count);
void gatherNodeRanges(IColumn & dst, const DirectGatherNode & node, const GatherRanges & ranges, size_t total_rows);

void gatherNullableRows(ColumnNullable & dst, const DirectGatherNode & node, const UInt64 * words, size_t count)
{
    /// The null map byte of a default row is 1: the generic path's `insertDefaultInto` inserts NULL,
    /// i.e. a set null byte over a zeroed nested default, which these two calls reproduce.
    gatherFixedDispatch<false>(dst.getNullMapColumn(), node.data_by_block.data(), 1, words, words + count, count, 1);
    gatherNodeRows(dst.getNestedColumn(), node.children[0], words, count);
}

void gatherStringRows(ColumnString & dst, const DirectGatherNode & node, const UInt64 * words, size_t count)
{
    const void * const * offsets_by_block = node.data_by_block.data();
    const void * const * chars_by_block = node.aux_by_block.data();

    ColumnString::Offsets & dst_offsets = dst.getOffsets();
    ColumnString::Chars & dst_chars = dst.getChars();
    const size_t old_rows = dst_offsets.size();
    dst_offsets.resize(old_rows + count);
    UInt64 * out_offsets = dst_offsets.data() + old_rows;
    /// `offsets[-1]` is the zeroed left padding, the usual "offset before the first row" idiom.
    UInt64 cursor = dst_offsets[static_cast<ssize_t>(old_rows) - 1];
    chassert(cursor == dst_chars.size());

    /// Pass 1: row lengths become destination offsets; a zero word is an empty string.
    for (size_t i = 0; i < count; ++i)
    {
        if (i + look_ahead < count)
        {
            const UInt64 ahead = words[i + look_ahead];
            if (ahead)
                __builtin_prefetch(static_cast<const UInt64 *>(offsets_by_block[refWordBlockNo(ahead)]) + refWordRowNo(ahead));
        }
        const UInt64 word = words[i];
        if (word)
        {
            const UInt64 * offsets = static_cast<const UInt64 *>(offsets_by_block[refWordBlockNo(word)]);
            const size_t row = refWordRowNo(word);
            cursor += offsets[row] - offsets[static_cast<ssize_t>(row) - 1];
        }
        out_offsets[i] = cursor;
    }

    /// Pass 2: copy the slices. The source offsets are cache-resident from pass 1, so the lead
    /// prefetch targets the character data.
    const size_t old_chars = dst_chars.size();
    dst_chars.resize(cursor);
    UInt8 * out_chars = dst_chars.data() + old_chars;
    for (size_t i = 0; i < count; ++i)
    {
        if (i + look_ahead < count)
        {
            const UInt64 ahead = words[i + look_ahead];
            if (ahead)
            {
                const UInt32 ahead_block = refWordBlockNo(ahead);
                const UInt64 * ahead_offsets = static_cast<const UInt64 *>(offsets_by_block[ahead_block]);
                __builtin_prefetch(
                    static_cast<const UInt8 *>(chars_by_block[ahead_block]) + ahead_offsets[static_cast<ssize_t>(refWordRowNo(ahead)) - 1]);
            }
        }
        const UInt64 word = words[i];
        if (!word)
            continue;
        const UInt32 block_no = refWordBlockNo(word);
        const size_t row = refWordRowNo(word);
        const UInt64 * offsets = static_cast<const UInt64 *>(offsets_by_block[block_no]);
        const UInt64 from = offsets[static_cast<ssize_t>(row) - 1];
        const UInt64 bytes = offsets[row] - from;
        /// Both chars arrays are padded (the destination was just resized, so its right pad is
        /// writable), which is the `ColumnString::insertFrom` copy idiom for short values.
        memcpySmallAllowReadWriteOverflow15(out_chars, static_cast<const UInt8 *>(chars_by_block[block_no]) + from, bytes);
        out_chars += bytes;
    }
    chassert(out_chars == dst_chars.data() + dst_chars.size());
}

void gatherArrayRows(ColumnArray & dst, const DirectGatherNode & node, const UInt64 * words, size_t count)
{
    const void * const * offsets_by_block = node.data_by_block.data();

    ColumnArray::Offsets & dst_offsets = dst.getOffsets();
    const size_t old_rows = dst_offsets.size();
    dst_offsets.resize(old_rows + count);
    UInt64 * out_offsets = dst_offsets.data() + old_rows;
    UInt64 cursor = dst_offsets[static_cast<ssize_t>(old_rows) - 1];

    GatherRanges child_ranges;
    child_ranges.reserve(count);
    size_t child_rows = 0;
    for (size_t i = 0; i < count; ++i)
    {
        if (i + look_ahead < count)
        {
            const UInt64 ahead = words[i + look_ahead];
            if (ahead)
                __builtin_prefetch(static_cast<const UInt64 *>(offsets_by_block[refWordBlockNo(ahead)]) + refWordRowNo(ahead));
        }
        const UInt64 word = words[i];
        if (word)
        {
            const UInt32 block_no = refWordBlockNo(word);
            const size_t row = refWordRowNo(word);
            const UInt64 * offsets = static_cast<const UInt64 *>(offsets_by_block[block_no]);
            const UInt64 from = offsets[static_cast<ssize_t>(row) - 1];
            const UInt64 length = offsets[row] - from;
            cursor += length;
            child_rows += length;
            if (length)
                appendGatherRange(child_ranges, block_no, from, length);
        }
        out_offsets[i] = cursor;
    }

    gatherNodeRanges(dst.getData(), node.children[0], child_ranges, child_rows);
}

void gatherTupleRows(ColumnTuple & dst, const DirectGatherNode & node, const UInt64 * words, size_t count)
{
    for (size_t i = 0; i < node.children.size(); ++i)
        gatherNodeRows(dst.getColumn(i), node.children[i], words, count);
}

void gatherVariantRows(ColumnVariant & dst, const DirectGatherNode & node, const UInt64 * words, size_t count)
{
    const size_t num_variants = node.children.size();
    const void * const * discriminators_by_block = node.data_by_block.data();
    const void * const * offsets_by_block = node.aux_by_block.data();

    /// The destination's local discriminator per global one; the source side is remapped per block
    /// through `local_to_global_by_block`, because local orders may differ between stored blocks.
    std::array<ColumnVariant::Discriminator, ColumnVariant::NULL_DISCRIMINATOR> dst_local_by_global;
    for (size_t g = 0; g < num_variants; ++g)
        dst_local_by_global[g] = dst.localDiscriminatorByGlobal(static_cast<ColumnVariant::Discriminator>(g));

    auto & dst_discriminators = dst.getLocalDiscriminators();
    auto & dst_offsets = dst.getOffsets();
    dst_discriminators.reserve(dst_discriminators.size() + count);
    dst_offsets.reserve(dst_offsets.size() + count);

    /// The rows of one global variant are collected as (block, in-variant row) words and gathered
    /// per child in a second step, reusing the flat encoding so the children stay oblivious.
    std::vector<PaddedPODArray<UInt64>> child_words(num_variants);
    std::vector<UInt64> child_sizes(num_variants);
    for (size_t g = 0; g < num_variants; ++g)
        child_sizes[g] = dst.getVariantByGlobalDiscriminator(g).size();

    /// Raw bases hoisted out of the loop: the hardened `std::vector` indexing otherwise reloads the
    /// base and re-checks the bounds on every row.
    const UInt8 * local_to_global = node.local_to_global_by_block.data();
    PaddedPODArray<UInt64> * child_words_by_global = child_words.data();
    UInt64 * child_sizes_by_global = child_sizes.data();

    for (size_t i = 0; i < count; ++i)
    {
        if (i + look_ahead < count)
        {
            const UInt64 ahead = words[i + look_ahead];
            if (ahead)
            {
                const UInt32 ahead_block = refWordBlockNo(ahead);
                __builtin_prefetch(static_cast<const UInt8 *>(discriminators_by_block[ahead_block]) + refWordRowNo(ahead));
                __builtin_prefetch(static_cast<const UInt64 *>(offsets_by_block[ahead_block]) + refWordRowNo(ahead));
            }
        }
        const UInt64 word = words[i];
        ColumnVariant::Discriminator local = ColumnVariant::NULL_DISCRIMINATOR;
        UInt32 block_no = 0;
        size_t row = 0;
        if (word)
        {
            block_no = refWordBlockNo(word);
            row = refWordRowNo(word);
            local = static_cast<const UInt8 *>(discriminators_by_block[block_no])[row];
        }
        if (local == ColumnVariant::NULL_DISCRIMINATOR)
        {
            /// `ColumnVariant::insertDefault`: a NULL row with a zero offset.
            dst_discriminators.push_back(ColumnVariant::NULL_DISCRIMINATOR);
            dst_offsets.push_back(0);
            continue;
        }
        const UInt8 global = local_to_global[block_no * num_variants + local];
        const UInt64 value_row = static_cast<const UInt64 *>(offsets_by_block[block_no])[row];
        dst_discriminators.push_back(dst_local_by_global[global]);
        dst_offsets.push_back(child_sizes_by_global[global]++);
        child_words_by_global[global].push_back(RowRef(block_no, value_row).encode());
    }

    for (size_t g = 0; g < num_variants; ++g)
        if (!child_words[g].empty())
            gatherNodeRows(dst.getVariantByGlobalDiscriminator(g), node.children[g], child_words[g].data(), child_words[g].size());
}

void gatherNullableRanges(ColumnNullable & dst, const DirectGatherNode & node, const GatherRanges & ranges, size_t total_rows)
{
    gatherRawRanges(dst.getNullMapColumn(), node.data_by_block.data(), 1, ranges, total_rows);
    gatherNodeRanges(dst.getNestedColumn(), node.children[0], ranges, total_rows);
}

void gatherStringRanges(ColumnString & dst, const DirectGatherNode & node, const GatherRanges & ranges, size_t total_rows)
{
    ColumnString::Offsets & dst_offsets = dst.getOffsets();
    ColumnString::Chars & dst_chars = dst.getChars();
    const size_t old_rows = dst_offsets.size();
    dst_offsets.resize(old_rows + total_rows);
    UInt64 * out_offsets = dst_offsets.data() + old_rows;
    UInt64 cursor = dst_offsets[static_cast<ssize_t>(old_rows) - 1];
    chassert(cursor == dst_chars.size());

    /// Pass 1: per-row destination offsets; a range's characters are one contiguous source run.
    for (const GatherRange & range : ranges)
        cursor += rebaseOffsets(static_cast<const UInt64 *>(node.data_by_block[range.block_no]), range, out_offsets, cursor).length;
    chassert(out_offsets == dst_offsets.data() + dst_offsets.size());

    /// Pass 2: one chars copy per range.
    const size_t old_chars = dst_chars.size();
    dst_chars.resize(cursor);
    UInt8 * out_chars = dst_chars.data() + old_chars;
    for (const GatherRange & range : ranges)
    {
        const UInt64 * offsets = static_cast<const UInt64 *>(node.data_by_block[range.block_no]);
        const UInt64 from = offsets[static_cast<ssize_t>(range.begin) - 1];
        const UInt64 bytes = offsets[range.begin + range.length - 1] - from;
        if (bytes)
            memcpy(out_chars, static_cast<const UInt8 *>(node.aux_by_block[range.block_no]) + from, bytes);
        out_chars += bytes;
    }
    chassert(out_chars == dst_chars.data() + dst_chars.size());
}

void gatherArrayRanges(ColumnArray & dst, const DirectGatherNode & node, const GatherRanges & ranges, size_t total_rows)
{
    ColumnArray::Offsets & dst_offsets = dst.getOffsets();
    const size_t old_rows = dst_offsets.size();
    dst_offsets.resize(old_rows + total_rows);
    UInt64 * out_offsets = dst_offsets.data() + old_rows;
    UInt64 cursor = dst_offsets[static_cast<ssize_t>(old_rows) - 1];

    GatherRanges child_ranges;
    child_ranges.reserve(ranges.size());
    size_t child_rows = 0;
    for (const GatherRange & range : ranges)
    {
        const GatherRange nested
            = rebaseOffsets(static_cast<const UInt64 *>(node.data_by_block[range.block_no]), range, out_offsets, cursor);
        cursor += nested.length;
        child_rows += nested.length;
        if (nested.length)
            appendGatherRange(child_ranges, nested.block_no, nested.begin, nested.length);
    }
    chassert(out_offsets == dst_offsets.data() + dst_offsets.size());

    gatherNodeRanges(dst.getData(), node.children[0], child_ranges, child_rows);
}

void gatherTupleRanges(ColumnTuple & dst, const DirectGatherNode & node, const GatherRanges & ranges, size_t total_rows)
{
    for (size_t i = 0; i < node.children.size(); ++i)
        gatherNodeRanges(dst.getColumn(i), node.children[i], ranges, total_rows);
}

void gatherVariantRanges(ColumnVariant & dst, const DirectGatherNode & node, const GatherRanges & ranges, size_t total_rows)
{
    /// A variant dispatches per row anyway (per-row discriminator), so expanding the ranges to flat
    /// row words loses nothing and reuses the row primitive.
    PaddedPODArray<UInt64> words;
    words.resize(total_rows);
    UInt64 * out = words.data();
    for (const GatherRange & range : ranges)
        for (UInt64 row = range.begin; row < range.begin + range.length; ++row)
            *out++ = RowRef(range.block_no, row).encode();
    chassert(out == words.data() + total_rows);
    gatherVariantRows(dst, node, words.data(), total_rows);
}

void gatherNodeRows(IColumn & dst, const DirectGatherNode & node, const UInt64 * words, size_t count)
{
    using enum DirectGatherNode::Kind;
    switch (node.kind)
    {
        case Fixed: gatherFixedDispatch<false>(dst, node.data_by_block.data(), node.stride, words, words + count, count, 0); break;
        case Nullable: gatherNullableRows(assert_cast<ColumnNullable &>(dst), node, words, count); break;
        case String: gatherStringRows(assert_cast<ColumnString &>(dst), node, words, count); break;
        case Array: gatherArrayRows(assert_cast<ColumnArray &>(dst), node, words, count); break;
        case Tuple: gatherTupleRows(assert_cast<ColumnTuple &>(dst), node, words, count); break;
        case Variant: gatherVariantRows(assert_cast<ColumnVariant &>(dst), node, words, count); break;
    }
}

void gatherNodeRanges(IColumn & dst, const DirectGatherNode & node, const GatherRanges & ranges, size_t total_rows)
{
    using enum DirectGatherNode::Kind;
    switch (node.kind)
    {
        case Fixed: gatherRawRanges(dst, node.data_by_block.data(), node.stride, ranges, total_rows); break;
        case Nullable: gatherNullableRanges(assert_cast<ColumnNullable &>(dst), node, ranges, total_rows); break;
        case String: gatherStringRanges(assert_cast<ColumnString &>(dst), node, ranges, total_rows); break;
        case Array: gatherArrayRanges(assert_cast<ColumnArray &>(dst), node, ranges, total_rows); break;
        case Tuple: gatherTupleRanges(assert_cast<ColumnTuple &>(dst), node, ranges, total_rows); break;
        case Variant: gatherVariantRanges(assert_cast<ColumnVariant &>(dst), node, ranges, total_rows); break;
    }
}

/// Fixed-width leaves whose `insertDefaultInto` writes bitwise zero, which is what the gather writes
/// for an unmatched row. `Date32` qualifies (0, not `getDefault`'s 1900-01-01); `Enum8`/`Enum16`
/// would get their first value instead, so this is keyed on `getTypeId` - `getColumnType` maps them
/// to `Int8`.
bool directGatherAdmitsLeaf(TypeIndex type_id)
{
    switch (type_id)
    {
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::UInt128:
        case TypeIndex::UInt256:
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Int128:
        case TypeIndex::Int256:
        case TypeIndex::BFloat16:
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
        case TypeIndex::Time:
        case TypeIndex::Time64:
        case TypeIndex::Interval:
        case TypeIndex::IPv4:
        case TypeIndex::IPv6:
        case TypeIndex::UUID:
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
        case TypeIndex::FixedString:
            return true;
        default:
            return false;
    }
}

}

template <bool from_row_list>
void gatherColumnDirect(
    IColumn & dst, const DirectGatherColumn & src, const UInt64 * row_refs_begin, const UInt64 * row_refs_end, size_t rows_to_add)
{
    chassert(src.node);
    const DirectGatherNode & node = *src.node;

    /// The dominant case - a fixed-width column with no replicated block - reads the ref words as
    /// they are; every other shape flattens them once (expanding row lists and applying the
    /// replicated row remap), so the recursive gather sees one inline-or-zero word per output row.
    if (node.kind == DirectGatherNode::Kind::Fixed && !src.remap_by_block)
    {
        gatherFixedDispatch<from_row_list>(dst, node.data_by_block.data(), node.stride, row_refs_begin, row_refs_end, rows_to_add, 0);
    }
    else
    {
        PaddedPODArray<UInt64> storage;
        const UInt64 * words = normalizeRefWords<from_row_list>(row_refs_begin, row_refs_end, src.remap_by_block, rows_to_add, storage);
        gatherNodeRows(dst, node, words, rows_to_add);
    }

    ProfileEvents::increment(ProfileEvents::HashJoinDirectGatheredValues, rows_to_add);
}

template void gatherColumnDirect<false>(IColumn &, const DirectGatherColumn &, const UInt64 *, const UInt64 *, size_t);
template void gatherColumnDirect<true>(IColumn &, const DirectGatherColumn &, const UInt64 *, const UInt64 *, size_t);

bool directGatherAdmits(const IDataType & type, const IColumn & destination, const DirectGatherNode & source)
{
    using enum DirectGatherNode::Kind;
    switch (source.kind)
    {
        case Fixed:
            return directGatherAdmitsLeaf(type.getTypeId()) && destination.isFixedAndContiguous()
                && destination.sizeOfValueIfFixed() == source.stride;
        case Nullable: {
            const auto * destination_nullable = typeid_cast<const ColumnNullable *>(&destination);
            const auto * type_nullable = typeid_cast<const DataTypeNullable *>(&type);
            return destination_nullable && type_nullable
                && directGatherAdmits(*type_nullable->getNestedType(), destination_nullable->getNestedColumn(), source.children[0]);
        }
        case String: return type.getTypeId() == TypeIndex::String && typeid_cast<const ColumnString *>(&destination);
        case Array: {
            const auto * destination_array = typeid_cast<const ColumnArray *>(&destination);
            const auto * type_array = typeid_cast<const DataTypeArray *>(&type);
            return destination_array && type_array
                && directGatherAdmits(*type_array->getNestedType(), destination_array->getData(), source.children[0]);
        }
        case Tuple: {
            const auto * destination_tuple = typeid_cast<const ColumnTuple *>(&destination);
            const auto * type_tuple = typeid_cast<const DataTypeTuple *>(&type);
            if (!destination_tuple || !type_tuple || destination_tuple->tupleSize() != source.children.size()
                || type_tuple->getElements().size() != source.children.size())
                return false;
            for (size_t i = 0; i < source.children.size(); ++i)
                if (!directGatherAdmits(*type_tuple->getElements()[i], destination_tuple->getColumn(i), source.children[i]))
                    return false;
            return true;
        }
        case Variant: {
            const auto * destination_variant = typeid_cast<const ColumnVariant *>(&destination);
            const auto * type_variant = typeid_cast<const DataTypeVariant *>(&type);
            if (!destination_variant || !type_variant || destination_variant->getNumVariants() != source.children.size()
                || type_variant->getVariants().size() != source.children.size())
                return false;
            /// `getVariants` is ordered by global discriminator, like `source.children`.
            for (size_t g = 0; g < source.children.size(); ++g)
                if (!directGatherAdmits(
                        *type_variant->getVariants()[g], destination_variant->getVariantByGlobalDiscriminator(g), source.children[g]))
                    return false;
            return true;
        }
    }

    return false;
}

}
