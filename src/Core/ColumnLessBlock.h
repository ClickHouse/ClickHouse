#pragma once

#include <Core/Block.h>

namespace DB
{

/// A `Block` takes its number of rows from its columns, so a block with no columns reports zero rows
/// and looks empty even when it carries rows that hold no values: see `BlockInfo::num_rows_without_columns`.
/// Every consumer of blocks that may have this form has to use these functions instead of `Block::rows`
/// and `Block::empty`, or the rows of such a block are silently lost.

/// The number of rows of the block, including the rows of a block with no columns.
inline size_t getNumRowsIncludingColumnLess(const Block & block)
{
    return block.columns() == 0 ? block.info.num_rows_without_columns : block.rows();
}

/// The block has neither columns nor rows: it is not data at all, but the absence of it,
/// e.g. the end of the data, or a timeout while waiting for it.
inline bool hasNoColumnsAndNoRows(const Block & block)
{
    return block.columns() == 0 && block.info.num_rows_without_columns == 0;
}

}
