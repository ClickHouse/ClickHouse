#include <Storages/MergeTree/RowOrderOptimizer.h>

#include <Interpreters/sortBlock.h>
#include <base/sort.h>
#include <Common/PODArray.h>
#include <Common/iota.h>
#include <Common/logger_useful.h>

#include <numeric>

namespace DB
{

namespace
{

/// Do the left and right row contain equal values in the sorting key columns (usually the primary key columns)
bool haveEqualSortingKeyValues(const Block & block, const SortDescription & sort_description, size_t left_row, size_t right_row)
{
    for (const auto & sort_column : sort_description)
    {
        const String & sort_col = sort_column.column_name;
        const IColumn & column = *block.getByName(sort_col).column;
        if (column.compareAt(left_row, right_row, column, 1) != 0)
            return false;
    }
    return true;
}

/// Returns the sorted indexes of all non-sorting-key columns.
std::vector<size_t> getOtherColumnIndexes(const Block & block, const SortDescription & sort_description)
{
    const size_t sorting_key_columns_count = sort_description.size();
    const size_t all_columns_count = block.columns();

    std::vector<size_t> other_column_indexes;
    other_column_indexes.reserve(all_columns_count - sorting_key_columns_count);

    if (sorting_key_columns_count == 0)
    {
        other_column_indexes.resize(block.columns());
        iota(other_column_indexes.begin(), other_column_indexes.end(), 0);
    }
    else
    {
        std::vector<size_t> sorted_column_indexes;
        sorted_column_indexes.reserve(sorting_key_columns_count);
        for (const SortColumnDescription & sort_column : sort_description)
        {
            size_t idx = block.getPositionByName(sort_column.column_name);
            sorted_column_indexes.emplace_back(idx);
        }
        ::sort(sorted_column_indexes.begin(), sorted_column_indexes.end());

        std::vector<size_t> all_column_indexes(all_columns_count);
        std::iota(all_column_indexes.begin(), all_column_indexes.end(), 0);
        std::set_difference(
            all_column_indexes.begin(),
            all_column_indexes.end(),
            sorted_column_indexes.begin(),
            sorted_column_indexes.end(),
            std::back_inserter(other_column_indexes));
    }
    chassert(other_column_indexes.size() == all_columns_count - sorting_key_columns_count);
    return other_column_indexes;
}

/// Returns a set of equal row ranges (equivalence classes) with the same row values for all sorting key columns (usually primary key columns.)
/// Example with 2 PK columns, 2 other columns --> 3 equal ranges
///          pk1    pk2    c1    c2
///          ----------------------
///          1      1     a     b
///          1      1     b     e
///          --------
///          1      2     e     a
///          1      2     d     c
///          1      2     e     a
///          --------
///          2      1     a     3
///          ----------------------
EqualRanges getEqualRanges(const Block & block, const SortDescription & sort_description, const IColumn::Permutation & permutation)
{
    EqualRanges ranges;
    const size_t rows = block.rows();
    if (sort_description.empty())
    {
        ranges.push_back({0, rows});
    }
    else
    {
        for (size_t i = 0; i < rows;)
        {
            size_t j = i;
            while (j < rows && haveEqualSortingKeyValues(block, sort_description, permutation[i], permutation[j]))
                ++j;
            ranges.push_back({i, j});
            i = j;
        }
    }
    return ranges;
}

std::vector<size_t> getCardinalitiesInPermutedRange(
    const Block & block,
    const std::vector<size_t> & other_column_indexes,
    const IColumn::Permutation & permutation,
    const EqualRange & equal_range)
{
    std::vector<size_t> cardinalities(other_column_indexes.size());
    for (size_t i = 0; i < other_column_indexes.size(); ++i)
    {
        const size_t column_id = other_column_indexes[i];
        const ColumnPtr & column = block.getByPosition(column_id).column;
        cardinalities[i] = column->estimateCardinalityInPermutedRange(permutation, equal_range);
    }
    return cardinalities;
}

void updatePermutationInEqualRange(
    const Block & block,
    const std::vector<size_t> & other_column_indexes,
    IColumn::Permutation & permutation,
    const EqualRange & equal_range,
    const std::vector<size_t> & cardinalities,
    const LoggerPtr & log)
{
    LOG_TEST(log, "Starting optimization in equal range");

    std::vector<size_t> column_order(other_column_indexes.size());
    iota(column_order.begin(), column_order.end(), 0);
    auto cmp = [&](size_t lhs, size_t rhs) -> bool { return cardinalities[lhs] < cardinalities[rhs]; };
    stable_sort(column_order.begin(), column_order.end(), cmp);

    std::vector<EqualRange> ranges = {equal_range};
    LOG_TEST(log, "equal_range: .from: {}, .to: {}", equal_range.from, equal_range.to);
    for (size_t i : column_order)
    {
        const size_t column_id = other_column_indexes[i];
        const ColumnPtr & column = block.getByPosition(column_id).column;
        LOG_TEST(log, "i: {}, column_id: {}, column type: {}, cardinality: {}", i, column_id, column->getName(), cardinalities[i]);
        column->updatePermutation(
            IColumn::PermutationSortDirection::Ascending, IColumn::PermutationSortStability::Stable, 0, 1, permutation, ranges);
    }

    LOG_TEST(log, "Finish optimization in equal range");
}

}

void RowOrderOptimizer::optimize(const Block & block, const SortDescription & sort_description, IColumn::Permutation & permutation)
{
    LoggerPtr log = getLogger("RowOrderOptimizer");

    LOG_TRACE(log, "Starting optimization");

    if (block.columns() == 0)
    {
        LOG_TRACE(log, "Finished optimization (block has no columns)");
        return; /// a table without columns, this should not happen in the first place ...
    }

    if (permutation.empty())
    {
        const size_t rows = block.rows();
        permutation.resize(rows);
        iota(permutation.data(), rows, IColumn::Permutation::value_type(0));
    }

    const EqualRanges equal_ranges = getEqualRanges(block, sort_description, permutation);
    const std::vector<size_t> other_columns_indexes = getOtherColumnIndexes(block, sort_description);

    LOG_TRACE(log, "columns: {}, sorting key columns: {}, rows: {}, equal ranges: {}", block.columns(), sort_description.size(), block.rows(), equal_ranges.size());

    for (const auto & equal_range : equal_ranges)
    {
        if (equal_range.size() <= 1)
            continue;
        const std::vector<size_t> cardinalities = getCardinalitiesInPermutedRange(block, other_columns_indexes, permutation, equal_range);
        updatePermutationInEqualRange(block, other_columns_indexes, permutation, equal_range, cardinalities, log);
    }

    LOG_TRACE(log, "Finished optimization");
}

}
