#pragma once

#include <Columns/IColumn.h>
#include <Core/TypeId.h>
#include <DataTypes/IDataType_fwd.h>
#include <Interpreters/RowRefs.h>

#include <limits>

namespace DB
{

/// A contiguous run of source rows in one block, or - when `block_no` is `default_rows` - a run of
/// unmatched rows. A reranged build side and the nested column of an `Array` are both made of runs,
/// and keeping them as runs is what makes those paths cost one copy per run rather than per row.
struct GatherRange
{
    static constexpr UInt32 default_rows = std::numeric_limits<UInt32>::max();

    UInt32 block_no = 0;
    UInt64 begin = 0;
    UInt64 length = 0;

    constexpr bool isDefault() const { return block_no == default_rows; }
};

using GatherRanges = PaddedPODArray<GatherRange>;

/// Buffers shared by all output columns of one emit call, because expanding a selection gives the same
/// answer for every column: `flat` is one inline-or-zero word per output row, `ranges` the same
/// selection as runs. A column stored as `ColumnReplicated` addresses the nested column instead, which
/// is a per-column row remap and so cannot be shared - it gets its own `remapped` pass over `flat`.
struct EmitScratch
{
    PaddedPODArray<UInt64> flat;
    PaddedPODArray<UInt64> remapped;
    GatherRanges ranges;
    bool flat_ready = false;
    bool ranges_ready = false;
};

/// How many output rows `words` expands to in `shape`, a zero word counting as one default row.
/// `RefWordShape::Flat` is one row per word by construction; the other two have to be walked.
/// `insertRawUninitialized` needs the exact count, which the emit builders' reserve hint only bounds.
[[nodiscard]] inline size_t countRefWordRows(std::span<const UInt64> words, RefWordShape shape)
{
    if (shape == RefWordShape::Flat)
        return words.size();

    size_t rows = 0;
    for (const UInt64 word : words)
        rows += word ? refWordRows(word) : 1;
    return rows;
}

/// Resolve block `block_no`'s planes of `node` from `column` and bind the kernel that will read them,
/// recursively. The first live block - recognized by a not-yet-set `column_type` - decides the node's
/// shape, and every later block has to be of the same concrete column class at every level, so the one
/// `typeid_cast` chain both classifies the first column and re-dispatches the rest.
///
/// `type` is the type of the destination the node will be gathered into. It is walked alongside the
/// column because the two answer different questions: the column decides which kernel reads it, the
/// type decides what an unmatched row writes. `default_from_type` goes false below a `Nullable`, where
/// the row is NULL and `ColumnNullable::insertDefault` fills the nested planes from the nested
/// *column*'s default rather than the nested type's.
///
/// Every encoding a stored right column can have is bound here, so there is nothing to refuse: a
/// column that does not resolve means the plan disagrees with the stored data, and that throws.
void resolveGatherNode(
    GatherNode & node,
    const DataTypePtr & type,
    const IColumn & column,
    size_t block_no,
    size_t num_blocks,
    bool default_from_type = true);

/// Append the `selection.rows` values of one output column, reading the encoded ref words in whichever
/// shape they arrive and the source through the planes `resolveGatherNode` resolved.
void gatherColumn(IColumn & dst, const GatherColumn & src, const RefWordSelection & selection, EmitScratch & scratch);

}
