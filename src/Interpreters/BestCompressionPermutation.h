#pragma once

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>

namespace DB
{

/* Selects equivalence classes on the lines in the block,
 * according to the current description and permutation satisfying it.
 */
EqualRanges getEqualRanges(const Block & block, const SortDescription & description, const IColumn::Permutation & permutation);

/* Tries to improve the permutation by reordering the block rows within the fixed equivalence classes according to description
 * so that the table is compressed in the best possible way.
 */
void getBestCompressionPermutation(const Block & block, const SortDescription & description, IColumn::Permutation & permutation);

}
