#pragma once

#include <Columns/IColumn.h>
#include <Core/TypeId.h>
#include <DataTypes/IDataType_fwd.h>
#include <Interpreters/RowRefs.h>

#include <optional>
#include <span>

namespace DB
{

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

/// Resolve block `block_no`'s planes of `node` from `column`, recursively. The first live block's
/// `IColumn::getPlanes` decides the node's shape and every later block has to be of the same concrete
/// class. `type` is walked alongside: the column decides which kernel reads it, the type decides what
/// an unmatched row writes. `default_from_type` goes false below a `Nullable`, where
/// `ColumnNullable::insertDefault` fills the nested planes from the nested *column*'s default.
void resolveGatherNode(
    GatherNode & node,
    const DataTypePtr & type,
    const IColumn & column,
    size_t block_no,
    size_t num_blocks,
    bool default_from_type = true);

/// Resolve block `block_no`'s planes of `node` from the row store field `access` of `row_store`.
/// Only a fixed-width value is kept in the row store, so the node's shape stops at `Fixed`.
void resolveRowStoreGatherNode(
    GatherNode & node,
    const DataTypePtr & type,
    const RowDataStore & row_store,
    const ColumnAccessIndex & access,
    size_t block_no,
    size_t num_blocks);

/// Append one output row per row of `selection` to every column of `columns` that `gather` (parallel
/// to it) has a resolved source for, reading the ref words in whichever shape they arrive and the
/// sources through the planes the resolvers resolved. A column with no source is left alone.
///
/// A column-major column takes one pass of its own over the whole selection. The row store ones are
/// swept together per L2-sized batch of output rows instead, because their fields share a row: one
/// pass over a batch of row store rows then serves every field of them, where a pass per field would
/// re-read the whole buffer. `row_store_row_length` is the row length of that store, which is the
/// same for every block and every field, and empty when no column of `gather` comes from one.
void gatherJoinOutputColumns(
    MutableColumns & columns,
    std::span<const GatherColumn> gather,
    const RefWordSelection & selection,
    std::optional<size_t> row_store_row_length);

}
