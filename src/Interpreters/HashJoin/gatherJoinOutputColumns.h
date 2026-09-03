#pragma once

#include <Columns/IColumn.h>
#include <Core/TypeId.h>
#include <DataTypes/IDataType_fwd.h>
#include <Interpreters/RowRefs.h>

#include <limits>

namespace DB
{

/// A contiguous run of source rows in one block, or - when `block_no` is `default_rows` - a run of
/// unmatched rows. A reranged build side and an `Array`'s nested column are both made of runs.
struct GatherRange
{
    static constexpr UInt32 default_rows = std::numeric_limits<UInt32>::max();

    UInt32 block_no = 0;
    UInt64 begin = 0;
    UInt64 length = 0;

    constexpr bool isDefault() const { return block_no == default_rows; }
};

using GatherRanges = PaddedPODArray<GatherRange>;

/// Buffers shared by all output columns of one emit call, since expanding a selection gives the same
/// answer for every column. A column stored as `ColumnReplicated` is the exception: its row remap is
/// per column, so it gets its own `remapped` pass over `flat`.
struct EmitScratch
{
    PaddedPODArray<UInt64> flat;
    PaddedPODArray<UInt64> remapped;
    GatherRanges ranges;
    bool flat_ready = false;
    bool ranges_ready = false;
};

/// How many output rows `words` expands to in `shape`, a zero word counting as one default row.
/// `insertRawUninitialized` needs the exact count, which the builders' reserve hint only bounds.
[[nodiscard]] inline size_t countRefWordRows(std::span<const UInt64> words, RefWordShape shape)
{
    if (shape == RefWordShape::Flat)
        return words.size();

    size_t rows = 0;
    for (const UInt64 word : words)
        rows += word ? refWordRows(word) : 1;
    return rows;
}

/// Resolve block `block_no`'s planes of `node` from `column`, recursively, and bind the kernel that
/// reads them. The first live block decides the node's shape and every later block has to be of the
/// same concrete class, so one `typeid_cast` chain both classifies the first column and dispatches
/// the rest. `type` is walked alongside because the column decides which kernel reads it while the
/// type decides what an unmatched row writes; `default_from_type` goes false below a `Nullable`,
/// where `ColumnNullable::insertDefault` fills the nested planes from the nested *column*'s default.
void resolveGatherNode(
    GatherNode & node,
    const DataTypePtr & type,
    const IColumn & column,
    size_t block_no,
    size_t num_blocks,
    bool default_from_type = true);

/// Append one output column's `selection.rows` values, reading the ref words in whichever shape
/// they arrive and the source through the planes `resolveGatherNode` resolved.
void gatherColumn(IColumn & dst, const GatherColumn & src, const RefWordSelection & selection, EmitScratch & scratch);

}
