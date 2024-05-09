#pragma once

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Columns/IColumn.h>

namespace DB
{

std::vector<size_t> getAlreadySortedColumnsIndex(const Block & block, const SortDescription & description);

std::vector<size_t> getNotAlreadySortedColumnsIndex(const Block & block, const SortDescription & description);

EqualRanges getEqualRanges(const Block & block, const SortDescription & description, IColumn::Permutation & permutation);

void getBestCompressionPermutation(const Block & block, const SortDescription & description, IColumn::Permutation & permutation);

}
