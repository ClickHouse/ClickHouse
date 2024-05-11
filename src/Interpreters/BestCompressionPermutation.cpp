#include <numeric>
#include <Interpreters/BestCompressionPermutation.h>

#include <Interpreters/sortBlock.h>
#include "Columns/IColumn.h"
#include "base/sort.h"
#include <Common/PODArray.h>

namespace DB
{

namespace
{

bool isEqual(const IColumn & column, size_t lhs, size_t rhs) 
{
    return column.compareAt(lhs, rhs, column, 1) == 0;
}

bool isEqual(const Block & block, const SortDescription & description, size_t lhs, size_t rhs) 
{
    for (const auto & column_description : description) 
    {
        const auto& column = *block.getByName(column_description.column_name).column;
        if (!isEqual(column, lhs, rhs)) 
        {
            return false;
        }
    }
    return true;
}

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

    std::cerr << "MYLOG estimate_unique_count = ";
    for (auto i : estimate_unique_count) {
        std::cerr << i << ", ";
    }
    std::cerr << std::endl;

    std::vector<EqualRange> equal_ranges{range};
    for (size_t i : order)
    {
        const size_t column_id = not_already_sorted_columns[i];
        std::cerr << "MYLOG column_id = " << column_id << std::endl;
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

EqualRanges getEqualRanges(const Block & block, const SortDescription & description, const IColumn::Permutation & permutation) {
    EqualRanges ranges;
    const ssize_t rows = block.rows();
    if (description.empty())
    {
        ranges.push_back({0, rows});
    }
    else 
    {
        for (ssize_t i = 0; i < rows; )
        {
            ssize_t j = i;
            for (; j < rows && isEqual(block, description, permutation[i], permutation[j]); ++j)
            {
            }
            ranges.push_back({i, j});
            i = j;
        }
    }
    return ranges;
}

void getBestCompressionPermutation(const Block & block, const SortDescription & description, IColumn::Permutation & permutation)
{  
    const auto equal_ranges = getEqualRanges(block, description, permutation);
    std::cerr << "MYLOG: equal_ranges = ";
    for (auto [l, r] : equal_ranges) {
        std::cerr << "(l = " << l << ", r = " << r << "), ";
    }
    std::cerr << std::endl;
    const auto not_already_sorted_columns = getNotAlreadySortedColumnsIndex(block, description);
    for (const auto & range : equal_ranges)
    {
        if (getRangeSize(range) <= 1)
            continue;
        getBestCompressionPermutationImpl(block, not_already_sorted_columns, permutation, range);
    }
}

}
