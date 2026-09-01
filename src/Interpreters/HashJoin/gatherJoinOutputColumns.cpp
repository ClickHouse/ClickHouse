#include <Interpreters/HashJoin/gatherJoinOutputColumns.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnQBit.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/IDataType.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>
#include <Common/memcpySmall.h>
#include <Common/typeid_cast.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// At a few nanoseconds of loop body per row, 32 rows of lead cover one source row's memory latency.
constexpr size_t look_ahead = 32;

/// A `Nullable`'s null map byte for an unmatched row: `insertDefault` inserts NULL.
constexpr char null_map_default = 1;

/// `STRIDE` is 0 when the width is only known at run time, which is what covers `FixedString(n)` for
/// an arbitrary `n`; a compile-time width turns the copy into a single load and store.
/// `default_pattern` is the `stride` bytes a zero ref word writes.
///
/// A run-time width stays a `memcpy` call. `memcpySmallAllowReadWriteOverflow15` is the obvious
/// alternative and it is slower here: measured on `FixedString(40)`, its 16-bytes-at-a-time loop
/// costs 4% to 8% more emit CPU than the call it saves.
template <bool from_row_list, size_t STRIDE>
void gatherFixedStride(
    IColumn & dst,
    const void * const * sources,
    size_t dynamic_stride,
    const UInt64 * row_refs_begin,
    const UInt64 * row_refs_end,
    size_t rows_to_add,
    const char * default_pattern)
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
            memcpy(out, default_pattern, stride);
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
    const char * default_pattern)
{
    switch (stride)
    {
#define M(STRIDE) \
    case (STRIDE): \
        gatherFixedStride<from_row_list, (STRIDE)>(dst, sources, stride, row_refs_begin, row_refs_end, rows_to_add, default_pattern); \
        break;
        M(1)
        M(2)
        M(4)
        M(8)
        M(16)
        M(32)
#undef M
        default:
            gatherFixedStride<from_row_list, 0>(dst, sources, stride, row_refs_begin, row_refs_end, rows_to_add, default_pattern);
            break;
    }
}

/// Remap one inline word's row through a replicated block's indexes: `row' = indexes[row]` addresses
/// the nested column. An identity entry (a block that stores the column plainly) passes the word through.
ALWAYS_INLINE UInt64 remapFlatWord(UInt64 word, const GatherRowRemap * remap_by_block)
{
    const GatherRowRemap & remap = remap_by_block[refWordBlockNo(word)];
    if (!remap.indexes_data)
        return word;
    const size_t row = refWordRowNo(word);
    size_t mapped = 0;
    switch (remap.index_width)
    {
        case 1: mapped = static_cast<const UInt8 *>(remap.indexes_data)[row]; break;
        case 2: mapped = static_cast<const UInt16 *>(remap.indexes_data)[row]; break;
        case 4: mapped = static_cast<const UInt32 *>(remap.indexes_data)[row]; break;
        default: mapped = static_cast<const UInt64 *>(remap.indexes_data)[row]; break;
    }
    /// Bounded where the block is taken, by `resolveEmitColumns`.
    chassert(mapped <= std::numeric_limits<UInt32>::max());
    return (word & 0xFFFFFFFF00000000ull) | static_cast<UInt32>(mapped);
}

/// Append a run to `ranges`, extending the last range when the run adjoins it: consecutive source
/// rows of one block (a row-list batch, a rerange) then become a single copy, and consecutive
/// unmatched rows a single run of defaults.
void appendGatherRange(GatherRanges & ranges, UInt32 block_no, UInt64 begin, UInt64 length)
{
    if (!ranges.empty() && ranges.back().block_no == block_no
        && (block_no == GatherRange::default_rows || ranges.back().begin + ranges.back().length == begin))
        ranges.back().length += length;
    else
        ranges.push_back(GatherRange{.block_no = block_no, .begin = begin, .length = length});
}

/// The selection as one inline-or-zero word per output row, addressing the nested column of every
/// replicated block. `RefWordShape::Flat` without a remap is already that and passes through; the
/// list and range shapes are expanded once per emit call into the scratch, which every
/// flat-consuming kernel of the call then shares.
const UInt64 * flatWords(const RefWordSelection & selection, const GatherRowRemap * remap_by_block, EmitScratch & scratch)
{
    const UInt64 * flat = selection.begin;
    if (selection.shape != RefWordShape::Flat)
    {
        if (!scratch.flat_ready)
        {
            scratch.flat.resize(selection.rows);
            UInt64 * out = scratch.flat.data();
            for (const UInt64 * word_i = selection.begin; word_i != selection.end; ++word_i)
            {
                if (!*word_i)
                    *out++ = 0;
                else
                    for (const UInt64 ref_word : refsOf(*word_i))
                        *out++ = ref_word;
            }
            chassert(out == scratch.flat.data() + selection.rows);
            scratch.flat_ready = true;
        }
        flat = scratch.flat.data();
    }

    if (!remap_by_block)
        return flat;

    scratch.remapped.resize(selection.rows);
    for (size_t i = 0; i < selection.rows; ++i)
        scratch.remapped[i] = flat[i] ? remapFlatWord(flat[i], remap_by_block) : 0;
    return scratch.remapped.data();
}

/// The selection as runs of source rows, built once per emit call. A range word is either an inline
/// ref - the rerange stores single-row keys that way - or a range node; a zero word is a run of one
/// unmatched row.
const GatherRanges & rangesOf(const RefWordSelection & selection, EmitScratch & scratch)
{
    if (!scratch.ranges_ready)
    {
        for (const UInt64 * word_i = selection.begin; word_i != selection.end; ++word_i)
        {
            if (!*word_i)
            {
                appendGatherRange(scratch.ranges, GatherRange::default_rows, 0, 1);
                continue;
            }
            const RowRefList ref_list = RowRefList::fromWord(*word_i);
            /// A non-range list node would be mis-emitted here as a run of consecutive rows.
            chassert(ref_list.isInline() || ref_list.asBatch()->is_range);
            const UInt64 start_word = ref_list.firstWord();
            appendGatherRange(scratch.ranges, refWordBlockNo(start_word), refWordRowNo(start_word), ref_list.rows());
        }
        scratch.ranges_ready = true;
    }
    return scratch.ranges;
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

/// One bulk copy per range from a per-block plane of `stride`-byte values into `dst`'s raw buffer; a
/// run of unmatched rows writes `default_pattern` per row.
///
/// `STRIDE` is specialized for the same reason as in `gatherFixedStride`: the range copy is one call
/// for a whole run and its size is long, but the default is written a row at a time, so a width known
/// at compile time is what turns that row into a single load and store rather than a call.
template <size_t STRIDE>
void gatherRawRangesStride(
    IColumn & dst,
    const void * const * bases,
    size_t dynamic_stride,
    const GatherRanges & ranges,
    size_t total_rows,
    const char * default_pattern)
{
    const size_t stride = STRIDE ? STRIDE : dynamic_stride;
    const std::span<char> out_span = dst.insertRawUninitialized(total_rows);
    chassert(out_span.size() == total_rows * stride);
    char * out = out_span.data();
    for (const GatherRange & range : ranges)
    {
        if (range.isDefault())
        {
            for (UInt64 i = 0; i < range.length; ++i, out += stride)
                memcpy(out, default_pattern, stride);
            continue;
        }
        const size_t bytes = range.length * stride;
        memcpy(out, static_cast<const char *>(bases[range.block_no]) + range.begin * stride, bytes);
        out += bytes;
    }
    chassert(out == out_span.data() + out_span.size());
}

void gatherRawRanges(
    IColumn & dst,
    const void * const * bases,
    size_t stride,
    const GatherRanges & ranges,
    size_t total_rows,
    const char * default_pattern)
{
    switch (stride)
    {
#define M(STRIDE) \
    case (STRIDE): \
        gatherRawRangesStride<(STRIDE)>(dst, bases, stride, ranges, total_rows, default_pattern); \
        break;
        M(1)
        M(2)
        M(4)
        M(8)
        M(16)
        M(32)
#undef M
        default:
            gatherRawRangesStride<0>(dst, bases, stride, ranges, total_rows, default_pattern);
            break;
    }
}

void gatherNodeRows(IColumn & dst, const GatherNode & node, const UInt64 * words, size_t count);
void gatherNodeRanges(IColumn & dst, const GatherNode & node, const GatherRanges & ranges, size_t total_rows);

/// The unmatched row of a `Kind::Rows` node: the output type's default, or the destination column's own
/// below a `Nullable`, where the row is NULL and the enclosing `insertDefault` owns the nested value.
void insertRowsDefault(IColumn & dst, const GatherNode & node)
{
    if (node.type)
        node.type->insertDefaultInto(dst);
    else
        dst.insertDefault();
}

void gatherNullableRows(ColumnNullable & dst, const GatherNode & node, const UInt64 * words, size_t count)
{
    /// A default row is NULL: a set null byte over the nested column's own default, which is what
    /// `ColumnNullable::insertDefault` writes and what the nested node's pattern reproduces.
    gatherFixedDispatch<false>(dst.getNullMapColumn(), node.data_by_block.data(), 1, words, words + count, count, &null_map_default);
    gatherNodeRows(dst.getNestedColumn(), node.children[0], words, count);
}

void gatherStringRows(ColumnString & dst, const GatherNode & node, const UInt64 * words, size_t count)
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

void gatherArrayRows(ColumnArray & dst, const GatherNode & node, const UInt64 * words, size_t count)
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

void gatherTupleRows(ColumnTuple & dst, const GatherNode & node, const UInt64 * words, size_t count)
{
    for (size_t i = 0; i < node.children.size(); ++i)
        gatherNodeRows(dst.getColumn(i), node.children[i], words, count);
}

void gatherVariantRows(ColumnVariant & dst, const GatherNode & node, const UInt64 * words, size_t count)
{
    const size_t num_variants = node.children.size();
    const void * const * discriminators_by_block = node.data_by_block.data();
    const void * const * offsets_by_block = node.aux_by_block.data();

    /// The destination's local discriminator per global one; the source side is remapped per block
    /// through `local_to_global_by_block`, because local orders may differ between stored blocks.
    std::array<ColumnVariant::Discriminator, ColumnVariant::NULL_DISCRIMINATOR> dst_local_by_global{};
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

/// One `insertRangeFrom` per run of consecutive source rows in one block, instead of one `insertFrom`
/// per row. A build side ordered by the join key stores a key's rows contiguously, so its refs form
/// one long run; a scattered one degrades to runs of length one for the cost of one comparison per
/// ref. Runs are tracked across the whole call, so a key-ordered build coalesces the whole selection.
void gatherRowsByRuns(IColumn & dst, const GatherNode & node, const UInt64 * words, size_t count)
{
    /// The plane kernels size their destination exactly through `insertRawUninitialized`; this one
    /// appends run by run, so it is the one that has to say how much is coming.
    dst.reserve(dst.size() + count);

    const void * const * sources = node.data_by_block.data();
    UInt32 run_block = 0;
    size_t run_begin = 0;
    size_t run_length = 0;

    auto flush_run = [&]
    {
        if (run_length)
            node.copy_rows(dst, *static_cast<const IColumn *>(sources[run_block]), run_begin, run_length);
        run_length = 0;
    };

    for (size_t i = 0; i < count; ++i)
    {
        const UInt64 word = words[i];
        if (!word)
        {
            flush_run();
            insertRowsDefault(dst, node);
            continue;
        }
        const UInt32 block_no = refWordBlockNo(word);
        const size_t row = refWordRowNo(word);
        if (run_length && block_no == run_block && row == run_begin + run_length)
        {
            ++run_length;
        }
        else
        {
            flush_run();
            run_block = block_no;
            run_begin = row;
            run_length = 1;
        }
    }
    flush_run();
}

void gatherNullableRanges(ColumnNullable & dst, const GatherNode & node, const GatherRanges & ranges, size_t total_rows)
{
    gatherRawRanges(dst.getNullMapColumn(), node.data_by_block.data(), 1, ranges, total_rows, &null_map_default);
    gatherNodeRanges(dst.getNestedColumn(), node.children[0], ranges, total_rows);
}

void gatherStringRanges(ColumnString & dst, const GatherNode & node, const GatherRanges & ranges, size_t total_rows)
{
    ColumnString::Offsets & dst_offsets = dst.getOffsets();
    ColumnString::Chars & dst_chars = dst.getChars();
    const size_t old_rows = dst_offsets.size();
    dst_offsets.resize(old_rows + total_rows);
    UInt64 * out_offsets = dst_offsets.data() + old_rows;
    UInt64 cursor = dst_offsets[static_cast<ssize_t>(old_rows) - 1];
    chassert(cursor == dst_chars.size());

    /// Pass 1: per-row destination offsets; a range's characters are one contiguous source run, and an
    /// unmatched row is the empty string, so its offset does not move.
    for (const GatherRange & range : ranges)
    {
        if (range.isDefault())
        {
            for (UInt64 i = 0; i < range.length; ++i)
                *out_offsets++ = cursor;
            continue;
        }
        cursor += rebaseOffsets(static_cast<const UInt64 *>(node.data_by_block[range.block_no]), range, out_offsets, cursor).length;
    }
    chassert(out_offsets == dst_offsets.data() + dst_offsets.size());

    /// Pass 2: one chars copy per range.
    const size_t old_chars = dst_chars.size();
    dst_chars.resize(cursor);
    UInt8 * out_chars = dst_chars.data() + old_chars;
    for (const GatherRange & range : ranges)
    {
        if (range.isDefault())
            continue;
        const UInt64 * offsets = static_cast<const UInt64 *>(node.data_by_block[range.block_no]);
        const UInt64 from = offsets[static_cast<ssize_t>(range.begin) - 1];
        const UInt64 bytes = offsets[range.begin + range.length - 1] - from;
        if (bytes)
            memcpy(out_chars, static_cast<const UInt8 *>(node.aux_by_block[range.block_no]) + from, bytes);
        out_chars += bytes;
    }
    chassert(out_chars == dst_chars.data() + dst_chars.size());
}

void gatherArrayRanges(ColumnArray & dst, const GatherNode & node, const GatherRanges & ranges, size_t total_rows)
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
        /// An unmatched row is the empty array, so nothing of it reaches the nested plane.
        if (range.isDefault())
        {
            for (UInt64 i = 0; i < range.length; ++i)
                *out_offsets++ = cursor;
            continue;
        }
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

void gatherTupleRanges(ColumnTuple & dst, const GatherNode & node, const GatherRanges & ranges, size_t total_rows)
{
    for (size_t i = 0; i < node.children.size(); ++i)
        gatherNodeRanges(dst.getColumn(i), node.children[i], ranges, total_rows);
}

void gatherVariantRanges(ColumnVariant & dst, const GatherNode & node, const GatherRanges & ranges, size_t total_rows)
{
    /// A variant dispatches per row anyway (per-row discriminator), so expanding the ranges to flat
    /// row words loses nothing and reuses the row primitive.
    PaddedPODArray<UInt64> words;
    words.resize(total_rows);
    UInt64 * out = words.data();
    for (const GatherRange & range : ranges)
    {
        if (range.isDefault())
            for (UInt64 i = 0; i < range.length; ++i)
                *out++ = 0;
        else
            for (UInt64 row = range.begin; row < range.begin + range.length; ++row)
                *out++ = RowRef(range.block_no, row).encode();
    }
    chassert(out == words.data() + total_rows);
    gatherVariantRows(dst, node, words.data(), total_rows);
}

/// A range already names a run, so it is one copy.
void gatherRowsByRanges(IColumn & dst, const GatherNode & node, const GatherRanges & ranges, size_t total_rows)
{
    dst.reserve(dst.size() + total_rows);

    for (const GatherRange & range : ranges)
    {
        if (range.isDefault())
            for (UInt64 i = 0; i < range.length; ++i)
                insertRowsDefault(dst, node);
        else
            node.copy_rows(dst, *static_cast<const IColumn *>(node.data_by_block[range.block_no]), range.begin, range.length);
    }
}

void gatherNodeRows(IColumn & dst, const GatherNode & node, const UInt64 * words, size_t count)
{
    using enum GatherNode::Kind;
    switch (node.kind)
    {
        case Fixed:
            gatherFixedDispatch<false>(
                dst, node.data_by_block.data(), node.stride, words, words + count, count, node.default_pattern.data());
            break;
        case Nullable: gatherNullableRows(assert_cast<ColumnNullable &>(dst), node, words, count); break;
        case String: gatherStringRows(assert_cast<ColumnString &>(dst), node, words, count); break;
        case Array: gatherArrayRows(assert_cast<ColumnArray &>(dst), node, words, count); break;
        case Tuple: gatherTupleRows(assert_cast<ColumnTuple &>(dst), node, words, count); break;
        case Variant: gatherVariantRows(assert_cast<ColumnVariant &>(dst), node, words, count); break;
        case Map: gatherNodeRows(assert_cast<ColumnMap &>(dst).getNestedColumn(), node.children[0], words, count); break;
        case Rows: gatherRowsByRuns(dst, node, words, count); break;
    }
}

void gatherNodeRanges(IColumn & dst, const GatherNode & node, const GatherRanges & ranges, size_t total_rows)
{
    using enum GatherNode::Kind;
    switch (node.kind)
    {
        case Fixed:
            gatherRawRanges(dst, node.data_by_block.data(), node.stride, ranges, total_rows, node.default_pattern.data());
            break;
        case Nullable: gatherNullableRanges(assert_cast<ColumnNullable &>(dst), node, ranges, total_rows); break;
        case String: gatherStringRanges(assert_cast<ColumnString &>(dst), node, ranges, total_rows); break;
        case Array: gatherArrayRanges(assert_cast<ColumnArray &>(dst), node, ranges, total_rows); break;
        case Tuple: gatherTupleRanges(assert_cast<ColumnTuple &>(dst), node, ranges, total_rows); break;
        case Variant: gatherVariantRanges(assert_cast<ColumnVariant &>(dst), node, ranges, total_rows); break;
        case Map:
            gatherNodeRanges(assert_cast<ColumnMap &>(dst).getNestedColumn(), node.children[0], ranges, total_rows);
            break;
        case Rows: gatherRowsByRanges(dst, node, ranges, total_rows); break;
    }
}

/// Every encoding a stored right column can have is bound above, and `ColumnConst` and `ColumnSparse`
/// are normalized away at the build boundary (`JoinCommon::materializeColumnsFromRightBlock`), so
/// getting here means the plan disagrees with the stored data - not that a slower path is wanted.
[[noreturn]] void throwNoGatherKernel(const IDataType & type, const IColumn & column, std::string_view reason)
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Join emit cannot read a right column of type {} stored as {}: {}",
        type.getName(),
        column.getFamilyName(),
        reason);
}

/// The emitted type has to be the same shape as the stored column at every level. Both names are in
/// the message, so there is nothing useful to add about how they differ.
[[noreturn]] void throwTypeDisagrees(const IDataType & type, const IColumn & column)
{
    throwNoGatherKernel(type, column, "the emitted type and the stored column are different shapes");
}

/// The `Kind::Rows` copy for one concrete column class. A run of length one goes through `insertFrom`
/// rather than `insertRangeFrom`: for `LowCardinality` the range form builds a used-keys mapping,
/// which costs far more than a single key translation, and a randomly ordered build side is nearly
/// all runs of length one.
template <typename Column>
void copyRowsConcrete(IColumn & dst, const IColumn & src, size_t begin, size_t length)
{
    auto & typed_dst = assert_cast<Column &>(dst);
    const auto & typed_src = assert_cast<const Column &>(src);
    if (length == 1)
        typed_dst.insertFrom(typed_src, begin);
    else
        typed_dst.insertRangeFrom(typed_src, begin, length);
}

/// One resolve step, holding what a `Kind::Rows` binding needs so that the encodings can be listed one
/// per line. `tryBind` answers whether `column` is that class, binding it when so; the source is then
/// the column itself rather than a plane of it, and an unmatched row comes from the output type.
///
/// Declared-type equality is the whole check these encodings need. Everything that varies between the
/// stored blocks - a `LowCardinality` dictionary, a `JSON` path set - is what `insertRangeFrom` on the
/// concrete class exists to reconcile.
struct RowsBinding
{
    GatherNode & node;
    const DataTypePtr & type;
    const IColumn & column;
    size_t block_no;
    size_t num_blocks;
    bool first;
    bool default_from_type;

    /// For a caller that has already established that both sides are `Column`.
    template <typename Column>
    void bind() const
    {
        if (first)
        {
            node.kind = GatherNode::Kind::Rows;
            node.data_by_block.resize(num_blocks);
            node.copy_rows = &copyRowsConcrete<Column>;
            node.type = default_from_type ? type : nullptr;
        }
        node.data_by_block[block_no] = &column;
    }

    /// Whether `column` is `Column`, in which case it is now bound.
    template <typename Column, typename Type>
    bool tryBind() const
    {
        if (!typeid_cast<const Column *>(&column))
            return false;
        if (!typeid_cast<const Type *>(type.get()))
            throwTypeDisagrees(*type, column);
        bind<Column>();
        return true;
    }
};

}

void resolveGatherNode(
    GatherNode & node,
    const DataTypePtr & type,
    const IColumn & column,
    size_t block_no,
    size_t num_blocks,
    bool default_from_type)
{
    using enum GatherNode::Kind;

    const bool first = !node.column_type;
    if (first)
        node.column_type = &typeid(column);
    else if (typeid(column) != *node.column_type)
        throwNoGatherKernel(*type, column, "the stored blocks hold it as different column classes");

    const RowsBinding rows{node, type, column, block_no, num_blocks, first, default_from_type};

    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get());
        if (!nullable_type)
            throwTypeDisagrees(*type, column);
        if (first)
        {
            node.kind = Nullable;
            node.data_by_block.resize(num_blocks);
            node.children.resize(1);
        }
        node.data_by_block[block_no] = nullable->getNullMapData().data();
        /// An unmatched row is NULL here, and `ColumnNullable::insertDefault` leaves the nested planes
        /// at the nested *column*'s default rather than the nested type's. The two differ for an `Enum`,
        /// and `assumeNotNull` on such a row observes the difference.
        resolveGatherNode(node.children[0], nullable_type->getNestedType(), nullable->getNestedColumn(), block_no, num_blocks, false);
        return;
    }

    if (const auto * string = typeid_cast<const ColumnString *>(&column))
    {
        if (type->getTypeId() != TypeIndex::String)
            throwTypeDisagrees(*type, column);
        if (first)
        {
            node.kind = String;
            node.data_by_block.resize(num_blocks);
            node.aux_by_block.resize(num_blocks);
        }
        node.data_by_block[block_no] = string->getOffsets().data();
        node.aux_by_block[block_no] = string->getChars().data();
        return;
    }

    if (const auto * array = typeid_cast<const ColumnArray *>(&column))
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
        if (!array_type)
            throwTypeDisagrees(*type, column);
        if (first)
        {
            node.kind = Array;
            node.data_by_block.resize(num_blocks);
            node.children.resize(1);
        }
        node.data_by_block[block_no] = array->getOffsets().data();
        /// An unmatched row is the empty array, so no default ever reaches the nested plane.
        resolveGatherNode(node.children[0], array_type->getNestedType(), array->getData(), block_no, num_blocks, false);
        return;
    }

    if (const auto * tuple = typeid_cast<const ColumnTuple *>(&column))
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
        if (!tuple_type || tuple_type->getElements().size() != tuple->tupleSize())
            throwTypeDisagrees(*type, column);
        /// An element-less tuple has no columns to walk - it keeps an explicit row count - so copying a
        /// row is a size bump, which is what its own `insertRangeFrom` does.
        if (tuple->tupleSize() == 0)
        {
            rows.bind<ColumnTuple>();
            return;
        }
        if (first)
        {
            node.kind = Tuple;
            node.children.resize(tuple->tupleSize());
        }
        for (size_t i = 0; i < node.children.size(); ++i)
            resolveGatherNode(
                node.children[i], tuple_type->getElements()[i], tuple->getColumn(i), block_no, num_blocks, default_from_type);
        return;
    }

    if (const auto * variant = typeid_cast<const ColumnVariant *>(&column))
    {
        const size_t num_variants = variant->getNumVariants();
        const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get());
        if (!variant_type || variant_type->getVariants().size() != num_variants)
            throwTypeDisagrees(*type, column);
        if (num_variants == 0 || num_variants >= ColumnVariant::NULL_DISCRIMINATOR)
            throwNoGatherKernel(*type, column, "the local discriminator does not fit beside the NULL one");
        /// `gatherVariantRows` names an in-variant position with a ref word, whose row field is 32 bits.
        /// Below an `Array` that position is an element index, so `addBlockToJoin`'s per-block row limit
        /// does not reach it; this is that same limit, counted in the elements the ref word will name.
        if (variant->size() > std::numeric_limits<UInt32>::max())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Too many Variant elements in right table block for HashJoin: {}", variant->size());
        if (first)
        {
            node.kind = Variant;
            node.data_by_block.resize(num_blocks);
            node.aux_by_block.resize(num_blocks);
            node.children.resize(num_variants);
            node.local_to_global_by_block.resize(num_blocks * num_variants);
        }
        node.data_by_block[block_no] = variant->getLocalDiscriminators().data();
        node.aux_by_block[block_no] = variant->getOffsets().data();
        for (size_t local = 0; local < num_variants; ++local)
            node.local_to_global_by_block[block_no * num_variants + local]
                = variant->globalDiscriminatorByLocal(static_cast<ColumnVariant::Discriminator>(local));
        /// `getVariants` is ordered by global discriminator, like `node.children`. An unmatched row is
        /// the NULL discriminator, so no default reaches a variant.
        for (size_t g = 0; g < num_variants; ++g)
            resolveGatherNode(
                node.children[g],
                variant_type->getVariants()[g],
                variant->getVariantByGlobalDiscriminator(g),
                block_no,
                num_blocks,
                false);
        return;
    }

    if (const auto * map = typeid_cast<const ColumnMap *>(&column))
    {
        const auto * map_type = typeid_cast<const DataTypeMap *>(type.get());
        if (!map_type)
            throwTypeDisagrees(*type, column);
        if (first)
        {
            node.kind = Map;
            node.children.resize(1);
        }
        /// A `Map` is its nested `Array(Tuple(key, value))` and nothing besides, so it delegates. An
        /// unmatched row is the empty map, which is the empty array the `Array` kernel already writes.
        resolveGatherNode(node.children[0], map_type->getNestedType(), map->getNestedColumn(), block_no, num_blocks, false);
        return;
    }

    /// The plane-less encodings: their rows are not an array of values, so only their own
    /// `insertRangeFrom` knows how to copy them - and that call is also the only thing that reconciles
    /// what differs between two stored blocks, which is a dictionary for `LowCardinality`, a path set
    /// for `JSON` and a variant list for `Dynamic`. `AggregateFunction` is here for ownership rather
    /// than layout: a row is a pointer to a state owned by exactly one source column, so copying the
    /// pointers across an output that spans many would leave them dangling or doubly freed, and that
    /// call is what keeps every source arena alive behind the output. `QBit` keeps its dimension and
    /// stride invariants inside the class, and `Nothing` has no values at all, so copying a row of it
    /// is a size bump. Declared-type equality is the whole check any of them needs.
    if (rows.tryBind<ColumnLowCardinality, DataTypeLowCardinality>())
        return;
    if (rows.tryBind<ColumnObject, DataTypeObject>())
        return;
    if (rows.tryBind<ColumnDynamic, DataTypeDynamic>())
        return;
    if (rows.tryBind<ColumnAggregateFunction, DataTypeAggregateFunction>())
        return;
    if (rows.tryBind<ColumnQBit, DataTypeQBit>())
        return;
    if (rows.tryBind<ColumnNothing, DataTypeNothing>())
        return;

    /// The fixed-width leaf, and the end of the line: anything that reaches it and is not a plain
    /// array of values has no kernel. `isFixedAndContiguous` is what keeps `getRawData` from throwing,
    /// and a `ColumnConst` must not pass even though it forwards that test to its one-element data
    /// column - it hands back a one-element buffer, whose base would be read out of bounds above row
    /// zero.
    if (first)
    {
        if (isColumnConst(column) || !column.isFixedAndContiguous())
            throwNoGatherKernel(*type, column, "no kernel is bound for this column class");
        node.kind = Fixed;
        node.stride = column.sizeOfValueIfFixed();
        node.data_by_block.resize(num_blocks);

        /// One default row of the destination, which doubles as the check that the type and the column
        /// really are the same fixed-width shape: the destination is a `cloneEmpty` of what
        /// `createColumn` builds, and `insertDefaultInto` writes what an unmatched row must get.
        MutableColumnPtr default_row = type->createColumn();
        if (default_from_type)
            type->insertDefaultInto(*default_row);
        else
            default_row->insertDefault();
        if (!default_row->isFixedAndContiguous() || default_row->sizeOfValueIfFixed() != node.stride
            || default_row->getRawData().size() != node.stride)
            throwTypeDisagrees(*type, column);
        const std::string_view default_bytes = default_row->getRawData();
        node.default_pattern.assign(default_bytes.begin(), default_bytes.end());
    }
    if (column.sizeOfValueIfFixed() != node.stride)
        throwNoGatherKernel(*type, column, "the stored blocks hold it at different widths");
    const std::string_view raw_data = column.getRawData();
    /// Any column that delegates `getRawData` hands back a buffer that does not hold its own rows,
    /// so require the size to account for exactly this column's rows.
    if (raw_data.size() != column.size() * node.stride)
        throwNoGatherKernel(*type, column, "its raw data does not hold exactly its own rows");
    node.data_by_block[block_no] = raw_data.data();
}

void gatherColumn(IColumn & dst, const GatherColumn & src, const RefWordSelection & selection, EmitScratch & scratch)
{
    chassert(src.node);
    const GatherNode & node = *src.node;

    /// A sorted build side names runs of rows rather than single rows, and keeping them as runs is the
    /// whole point of having reranged it: flattening would cost a word and a copy per row instead of
    /// per run. A replicated source is the exception, because `row' = indexes[row]` breaks any run.
    if (selection.shape == RefWordShape::Ranges && !src.remap_by_block)
    {
        gatherNodeRanges(dst, node, rangesOf(selection, scratch), selection.rows);
    }
    /// The dominant case - a fixed-width column with no replicated block - reads the flat and list
    /// shapes as they are; every other kernel needs one inline-or-zero word per output row, which
    /// `flatWords` produces once per emit call for all of them to share.
    else if (node.kind == GatherNode::Kind::Fixed && !src.remap_by_block)
    {
        if (selection.shape == RefWordShape::Flat)
            gatherFixedDispatch<false>(
                dst, node.data_by_block.data(), node.stride, selection.begin, selection.end, selection.rows,
                node.default_pattern.data());
        else
            gatherFixedDispatch<true>(
                dst, node.data_by_block.data(), node.stride, selection.begin, selection.end, selection.rows,
                node.default_pattern.data());
    }
    else
    {
        gatherNodeRows(dst, node, flatWords(selection, src.remap_by_block, scratch), selection.rows);
    }
}

}
