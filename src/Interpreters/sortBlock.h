#pragma once

#include <Columns/IColumn.h>
#include <base/types.h>
#include <Common/PODArray_fwd.h>

namespace DB
{

class Block;
class SortDescription;
using IColumnPermutation = PaddedPODArray<size_t>;

/// Sort one block by `description`. If limit != 0, then the partial sort of the first `limit` rows is produced.
void sortBlock(Block & block, const SortDescription & description, UInt64 limit = 0, IColumn::PermutationSortStability stability = IColumn::PermutationSortStability::Unstable);

/** Same as sortBlock, but do not sort the block, but only calculate the permutation of the values,
  *  so that you can rearrange the column values yourself.
  * Sorting is stable. This is important for keeping the order of rows in the CollapsingMergeTree engine
  *  - because based on the order of rows it is determined whether to delete or leave groups of rows when collapsing.
  * Used only in StorageMergeTree to sort the data with INSERT.
  */
void stableGetPermutation(const Block & block, const SortDescription & description, IColumnPermutation & out_permutation);

/** Quickly check whether the block is already sorted. If the block is not sorted - returns false as fast as possible.
  */
bool isAlreadySorted(const Block & block, const SortDescription & description);

}
