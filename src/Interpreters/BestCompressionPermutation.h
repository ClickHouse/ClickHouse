#pragma once

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>

namespace DB
{

EqualRanges getEqualRanges(const Block & block, const SortDescription & description, const IColumn::Permutation & permutation);

void getBestCompressionPermutation(const Block & block, const SortDescription & description, IColumn::Permutation & permutation);

}
