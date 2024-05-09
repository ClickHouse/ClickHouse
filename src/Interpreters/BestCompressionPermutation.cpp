#include <numeric>
#include <Interpreters/BestCompressionPermutation.h>

#include <Interpreters/sortBlock.h>
#include "Columns/IColumn.h"
#include "base/sort.h"

namespace DB
{

namespace
{

void getBestCompressionPermutationImpl(
    const Block & block,
    const std::vector<size_t> & not_already_sorted_columns,
    IColumn::Permutation & permutation,
    const EqualRange & range)
{
    std::vector<size_t> estimate_unique_count(not_already_sorted_columns.size());
    for (size_t i = 0; i < not_already_sorted_columns.size(); ++i)
    {
        const auto column = block.getByPosition(i).column;
        // TODO: improve with sampling
        estimate_unique_count[i] = column->estimateNumberOfDifferent(permutation, range, -1);
    }

    std::vector<size_t> order(not_already_sorted_columns.size());
    std::iota(order.begin(), order.end(), 0);

    auto comparator = [&](size_t lhs, size_t rhs) -> bool { return estimate_unique_count[lhs] < estimate_unique_count[rhs]; };

    ::sort(order.begin(), order.end(), comparator);

    std::vector<EqualRange> equal_ranges{range};
    for (size_t i : order)
    {
        const size_t column_id = not_already_sorted_columns[i];
        const auto column = block.getByPosition(column_id).column;
        column->updatePermutationForCompression(permutation, equal_ranges);
    }
}

}

std::vector<size_t> getAlreadySortedColumnsIndex(const Block & block, const SortDescription & description)
{
    std::vector<size_t> already_sorted_columns;
    already_sorted_columns.reserve(description.size());
    for (const SortColumnDescription & column_description : description)
    {
        size_t id = block.getPositionByName(column_description.column_name);
        already_sorted_columns.emplace_back(id);
    }
    ::sort(already_sorted_columns.begin(), already_sorted_columns.end());
    return already_sorted_columns;
}

std::vector<size_t> getNotAlreadySortedColumnsIndex(const Block & block, const SortDescription & description)
{
    std::vector<size_t> not_already_sorted_columns;
    not_already_sorted_columns.reserve(block.columns() - description.size());
    if (description.empty())
    {
        not_already_sorted_columns.resize(block.columns());
        std::iota(not_already_sorted_columns.begin(), not_already_sorted_columns.end(), 0);
    }
    else
    {
        const auto already_sorted_columns = getAlreadySortedColumnsIndex(block, description);
        for (size_t i = 0; i < already_sorted_columns.front(); ++i)
            not_already_sorted_columns.push_back(i);
        for (size_t i = 0; i + 1 < already_sorted_columns.size(); ++i)
            for (size_t id = already_sorted_columns[i] + 1; id < already_sorted_columns[i + 1]; ++id)
                not_already_sorted_columns.push_back(id);
        for (size_t i = already_sorted_columns.back() + 1; i < block.columns(); ++i)
            not_already_sorted_columns.push_back(i);
    }
    return not_already_sorted_columns;
}

void getBestCompressionPermutation(const Block & block, const SortDescription & description, IColumn::Permutation & permutation)
{
    const auto equal_ranges = getEqualRanges(block, description, permutation);
    const auto not_already_sorted_columns = getNotAlreadySortedColumnsIndex(block, description);
    for (const auto & range : equal_ranges)
    {
        if (getRangeSize(range) <= 1)
            continue;
        getBestCompressionPermutationImpl(block, not_already_sorted_columns, permutation, range);
    }
}

}
