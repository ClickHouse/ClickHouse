#include <Interpreters/sortBlock.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Functions/FunctionHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_COLLATION;
}

/// Column with description for sort
struct ColumnWithSortDescription
{
    const IColumn * column = nullptr;
    SortColumnDescription description;

    /// It means, that this column is ColumnConst
    bool column_const = false;
};

using ColumnsWithSortDescriptions = std::vector<ColumnWithSortDescription>;

namespace
{

inline bool isCollationRequired(const SortColumnDescription & description)
{
    return description.collator != nullptr;
}

template <bool check_collation>
struct PartialSortingLessImpl
{
    const ColumnsWithSortDescriptions & columns;

    explicit PartialSortingLessImpl(const ColumnsWithSortDescriptions & columns_) : columns(columns_) { }

    inline bool operator()(size_t a, size_t b) const
    {
        for (const auto & elem : columns)
        {
            int res;

            if (elem.column_const)
            {
                res = 0;
                continue;
            }

            if constexpr (check_collation)
            {
                if (isCollationRequired(elem.description))
                {
                    res = elem.column->compareAtWithCollation(a, b, *elem.column, elem.description.nulls_direction, *elem.description.collator);
                }
                else
                {
                    res = elem.column->compareAt(a, b, *elem.column, elem.description.nulls_direction);
                }
            }
            else
            {
                res = elem.column->compareAt(a, b, *elem.column, elem.description.nulls_direction);
            }

            res *= elem.description.direction;
            if (res < 0)
                return true;
            else if (res > 0)
                return false;
        }
        return false;
    }
};

using PartialSortingLess = PartialSortingLessImpl<false>;
using PartialSortingLessWithCollation = PartialSortingLessImpl<true>;

}

void convertTupleColumnIntoSortDescriptions(
    const ColumnTuple * tuple, const SortColumnDescription & description, ColumnsWithSortDescriptions & result)
{
    for (const auto & column : tuple->getColumns())
    {
        if (const auto * subtuple = typeid_cast<const ColumnTuple *>(column.get()))
        {
            convertTupleColumnIntoSortDescriptions(subtuple, description, result);
        }
        else
        {
            result.emplace_back(ColumnWithSortDescription{column.get(), description, isColumnConst(*column)});

            if (isCollationRequired(description) && !result.back().column->isCollationSupported())
                result.back().description.collator = nullptr;
        }
    }
}

ColumnsWithSortDescriptions getColumnsWithSortDescription(const Block & block, const SortDescription & description)
{
    size_t size = description.size();

    ColumnsWithSortDescriptions result;
    result.reserve(size);

    for (size_t i = 0; i < size; ++i)
    {
        const auto & sort_column_description = description[i];

        const IColumn * column = !sort_column_description.column_name.empty()
            ? block.getByName(sort_column_description.column_name).column.get()
            : block.safeGetByPosition(sort_column_description.column_number).column.get();

        if (isCollationRequired(sort_column_description))
        {
            if (!column->isCollationSupported())
                throw Exception(
                    "Collations could be specified only for String, LowCardinality(String), Nullable(String) or for Array or Tuple, "
                    "containing them.",
                    ErrorCodes::BAD_COLLATION);
        }

        if (const auto * tuple = typeid_cast<const ColumnTuple *>(column))
            convertTupleColumnIntoSortDescriptions(tuple, sort_column_description, result);
        else
            result.emplace_back(ColumnWithSortDescription{column, sort_column_description, isColumnConst(*column)});
    }

    return result;
}

void sortBlock(Block & block, const SortDescription & description, UInt64 limit)
{
    if (!block)
        return;

    ColumnsWithSortDescriptions columns_with_sort_descriptions = getColumnsWithSortDescription(block, description);

    bool all_const = true;
    for (const auto & column : columns_with_sort_descriptions)
    {
        if (!column.column_const)
        {
            all_const = false;
            break;
        }
    }
    if (all_const)
        return;

    IColumn::Permutation permutation;

    /// If only one column to sort by
    if (columns_with_sort_descriptions.size() == 1)
    {
        auto & column_with_sort_description = columns_with_sort_descriptions[0];

        bool reverse = column_with_sort_description.description.direction == -1;
        int nan_direction_hint = column_with_sort_description.description.nulls_direction;
        const auto & column = column_with_sort_description.column;

        if (isCollationRequired(column_with_sort_description.description))
            column->getPermutationWithCollation(
                *column_with_sort_description.description.collator, reverse, limit, nan_direction_hint, permutation);
        else
            column->getPermutation(reverse, limit, nan_direction_hint, permutation);
    }
    else
    {
        size_t size = block.rows();
        permutation.resize(size);
        for (size_t i = 0; i < size; ++i)
            permutation[i] = i;

        if (limit >= size)
            limit = 0;

        EqualRanges ranges;
        ranges.emplace_back(0, permutation.size());

        for (const auto & column_with_sort_description : columns_with_sort_descriptions)
        {
            while (!ranges.empty() && limit && limit <= ranges.back().first)
                ranges.pop_back();

            if (ranges.empty())
                break;

            if (column_with_sort_description.column_const)
                continue;

            bool is_collation_required = isCollationRequired(column_with_sort_description.description);
            bool reverse = column_with_sort_description.description.direction < 0;
            int nan_direction_hint = column_with_sort_description.description.nulls_direction;
            const auto & column = column_with_sort_description.column;

            if (is_collation_required)
            {
                column->updatePermutationWithCollation(
                    *column_with_sort_description.description.collator, reverse, limit, nan_direction_hint, permutation, ranges);
            }
            else
            {
                column->updatePermutation(reverse, limit, nan_direction_hint, permutation, ranges);
            }
        }
    }

    size_t columns = block.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        auto & column_to_sort = block.getByPosition(i).column;
        column_to_sort = column_to_sort->permute(permutation, limit);
    }
}

void stableGetPermutation(const Block & block, const SortDescription & description, IColumn::Permutation & out_permutation)
{
    if (!block)
        return;

    size_t size = block.rows();
    out_permutation.resize(size);
    for (size_t i = 0; i < size; ++i)
        out_permutation[i] = i;

    ColumnsWithSortDescriptions columns_with_sort_desc = getColumnsWithSortDescription(block, description);

    std::stable_sort(out_permutation.begin(), out_permutation.end(), PartialSortingLess(columns_with_sort_desc));
}

bool isAlreadySorted(const Block & block, const SortDescription & description)
{
    if (!block)
        return true;

    size_t rows = block.rows();

    ColumnsWithSortDescriptions columns_with_sort_desc = getColumnsWithSortDescription(block, description);

    PartialSortingLess less(columns_with_sort_desc);

    /** If the rows are not too few, then let's make a quick attempt to verify that the block is not sorted.
     * Constants - at random.
     */
    static constexpr size_t num_rows_to_try = 10;
    if (rows > num_rows_to_try * 5)
    {
        for (size_t i = 1; i < num_rows_to_try; ++i)
        {
            size_t prev_position = rows * (i - 1) / num_rows_to_try;
            size_t curr_position = rows * i / num_rows_to_try;

            if (less(curr_position, prev_position))
                return false;
        }
    }

    for (size_t i = 1; i < rows; ++i)
        if (less(i, i - 1))
            return false;

    return true;
}


void stableSortBlock(Block & block, const SortDescription & description)
{
    if (!block)
        return;

    IColumn::Permutation permutation;
    stableGetPermutation(block, description, permutation);

    size_t columns = block.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        auto & column_to_sort = block.safeGetByPosition(i).column;
        column_to_sort = column_to_sort->permute(permutation, 0);
    }
}

}
