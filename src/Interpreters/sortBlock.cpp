#include <Interpreters/sortBlock.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <Functions/FunctionHelpers.h>

#include <pdqsort.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_COLLATION;
}

static bool isCollationRequired(const SortColumnDescription & description)
{
    return description.collator != nullptr;
}


ColumnsWithSortDescriptions getColumnsWithSortDescription(const Block & block, const SortDescription & description)
{
    size_t size = description.size();
    ColumnsWithSortDescriptions res;
    res.reserve(size);

    for (size_t i = 0; i < size; ++i)
    {
        const IColumn * column = !description[i].column_name.empty()
            ? block.getByName(description[i].column_name).column.get()
            : block.safeGetByPosition(description[i].column_number).column.get();

        res.emplace_back(ColumnWithSortDescription{column, description[i], isColumnConst(*column)});
    }

    return res;
}


struct PartialSortingLess
{
    const ColumnsWithSortDescriptions & columns;

    explicit PartialSortingLess(const ColumnsWithSortDescriptions & columns_) : columns(columns_) {}

    bool operator() (size_t a, size_t b) const
    {
        for (const auto & elem : columns)
        {
            int res;
            if (elem.column_const)
                res = 0;
            else
                res = elem.description.direction * elem.column->compareAt(a, b, *elem.column, elem.description.nulls_direction);
            if (res < 0)
                return true;
            else if (res > 0)
                return false;
        }
        return false;
    }
};


struct PartialSortingLessWithCollation
{
    const ColumnsWithSortDescriptions & columns;

    explicit PartialSortingLessWithCollation(const ColumnsWithSortDescriptions & columns_)
        : columns(columns_)
    {
    }

    bool operator() (size_t a, size_t b) const
    {
        for (const auto & elem : columns)
        {
            int res;

            if (elem.column_const)
            {
                res = 0;
            }
            else if (isCollationRequired(elem.description))
            {
                const ColumnString & column_string = assert_cast<const ColumnString &>(*elem.column);
                res = column_string.compareAtWithCollation(a, b, *elem.column, *elem.description.collator);
            }
            else
                res = elem.column->compareAt(a, b, *elem.column, elem.description.nulls_direction);
            res *= elem.description.direction;
            if (res < 0)
                return true;
            else if (res > 0)
                return false;
        }
        return false;
    }
};


void sortBlock(Block & block, const SortDescription & description, UInt64 limit)
{
    if (!block)
        return;

    /// If only one column to sort by
    if (description.size() == 1)
    {
        IColumn::Permutation perm;
        bool reverse = description[0].direction == -1;

        const IColumn * column = !description[0].column_name.empty()
            ? block.getByName(description[0].column_name).column.get()
            : block.safeGetByPosition(description[0].column_number).column.get();

        bool is_column_const = false;
        if (isCollationRequired(description[0]))
        {
            /// it it's real string column, than we need sort
            if (const ColumnString * column_string = checkAndGetColumn<ColumnString>(column))
                column_string->getPermutationWithCollation(*description[0].collator, reverse, limit, perm);
            else if (checkAndGetColumnConstData<ColumnString>(column))
                is_column_const = true;
            else
                throw Exception("Collations could be specified only for String columns.", ErrorCodes::BAD_COLLATION);

        }
        else if (!isColumnConst(*column))
        {
            int nan_direction_hint = description[0].nulls_direction;
            auto special_sort = description[0].special_sort;

            if (special_sort == SpecialSort::OPENCL_BITONIC)
                column->getSpecialPermutation(reverse, limit, nan_direction_hint, perm, IColumn::SpecialSort::OPENCL_BITONIC);
            else
                column->getPermutation(reverse, limit, nan_direction_hint, perm);
        }
        else
            /// we don't need to do anything with const column
            is_column_const = true;

        size_t columns = block.columns();
        for (size_t i = 0; i < columns; ++i)
        {
            if (!is_column_const)
                block.getByPosition(i).column = block.getByPosition(i).column->permute(perm, limit);
        }
    }
    else
    {
        size_t size = block.rows();
        IColumn::Permutation perm(size);
        for (size_t i = 0; i < size; ++i)
            perm[i] = i;

        if (limit >= size)
            limit = 0;

        bool need_collation = false;
        ColumnsWithSortDescriptions columns_with_sort_desc = getColumnsWithSortDescription(block, description);

        for (size_t i = 0, num_sort_columns = description.size(); i < num_sort_columns; ++i)
        {
            const IColumn * column = columns_with_sort_desc[i].column;
            if (isCollationRequired(description[i]))
            {
                if (!checkAndGetColumn<ColumnString>(column) && !checkAndGetColumnConstData<ColumnString>(column))
                    throw Exception("Collations could be specified only for String columns.", ErrorCodes::BAD_COLLATION);

                need_collation = true;
            }
        }

        if (need_collation)
        {
            EqualRanges ranges;
            ranges.emplace_back(0, perm.size());
            for (const auto & column : columns_with_sort_desc)
            {
                while (!ranges.empty() && limit && limit <= ranges.back().first)
                    ranges.pop_back();

                if (ranges.empty())
                    break;

                if (column.column_const)
                    continue;

                if (isCollationRequired(column.description))
                {
                    const ColumnString & column_string = assert_cast<const ColumnString &>(*column.column);
                    column_string.updatePermutationWithCollation(
                        *column.description.collator,
                        column.description.direction < 0, limit, column.description.nulls_direction, perm, ranges);
                }
                else
                {
                    column.column->updatePermutation(
                        column.description.direction < 0, limit, column.description.nulls_direction, perm, ranges);
                }
            }
        }
        else
        {
            EqualRanges ranges;
            ranges.emplace_back(0, perm.size());
            for (const auto & column : columns_with_sort_desc)
            {
                while (!ranges.empty() && limit && limit <= ranges.back().first)
                {
                    ranges.pop_back();
                }
                if (ranges.empty())
                {
                    break;
                }
                column.column->updatePermutation(
                    column.description.direction < 0, limit, column.description.nulls_direction, perm, ranges);
            }
        }

        size_t columns = block.columns();
        for (size_t i = 0; i < columns; ++i)
        {
            block.getByPosition(i).column = block.getByPosition(i).column->permute(perm, limit);
        }
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

    IColumn::Permutation perm;
    stableGetPermutation(block, description, perm);

    size_t columns = block.columns();
    for (size_t i = 0; i < columns; ++i)
        block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->permute(perm, 0);
}

}
