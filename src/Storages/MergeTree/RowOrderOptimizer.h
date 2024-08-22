#pragma once

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>

namespace DB
{

class RowOrderOptimizer
{
public:
    /// Given the columns in a Block with a sub-set of them as sorting key columns (usually primary key columns --> SortDescription), and a
    /// permutation of the rows, this function tries to "improve" the permutation such that the data can be compressed better by generic
    /// compression algorithms such as zstd. The heuristics is based on D. Lemire, O. Kaser (2011): Reordering columns for smaller
    /// indexes, https://doi.org/10.1016/j.ins.2011.02.002
    /// The algorithm works like this:
    /// - Divide the sorting key columns horizontally into "equal ranges". An equal range is defined by the same sorting key values on all
    ///   of its rows. We can re-shuffle the non-sorting-key values within each equal range freely.
    /// - Determine (estimate) for each equal range the cardinality of each non-sorting-key column.
    /// - The simple heuristics applied is that non-sorting key columns will be sorted (within each equal range) in order of ascending
    ///   cardinality. This maximizes the length of equal-value runs within the non-sorting-key columns, leading to better compressability.
    static void optimize(const Block & block, const SortDescription & sort_description, IColumn::Permutation & permutation);
};

}
