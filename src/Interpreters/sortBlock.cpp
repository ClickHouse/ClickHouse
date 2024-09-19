#include <Interpreters/sortBlock.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Common/iota.h>

#ifdef __SSE2__
    #include <emmintrin.h>
#endif

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

    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        int res = 0;

        for (const auto & elem : columns)
        {
            if (elem.column_const)
            {
                continue;
            }

            if constexpr (check_collation)
            {
                if (isCollationRequired(elem.description))
                {
                    res = elem.column->compareAtWithCollation(lhs, rhs, *elem.column, elem.description.nulls_direction, *elem.description.collator);
                }
                else
                {
                    res = elem.column->compareAt(lhs, rhs, *elem.column, elem.description.nulls_direction);
                }
            }
            else
            {
                res = elem.column->compareAt(lhs, rhs, *elem.column, elem.description.nulls_direction);
            }

            res *= elem.description.direction;

            if (res != 0)
                break;
        }

        return res;
    }

    ALWAYS_INLINE bool operator()(size_t lhs, size_t rhs) const
    {
        int res = compare(lhs, rhs);
        return res < 0;
    }
};

using PartialSortingLess = PartialSortingLessImpl<false>;
using PartialSortingLessWithCollation = PartialSortingLessImpl<true>;

ColumnsWithSortDescriptions getColumnsWithSortDescription(const Block & block, const SortDescription & description)
{
    size_t size = description.size();

    ColumnsWithSortDescriptions result;
    result.reserve(size);

    for (size_t i = 0; i < size; ++i)
    {
        const auto & sort_column_description = description[i];

        const IColumn * column = block.getByName(sort_column_description.column_name).column.get();

        if (isCollationRequired(sort_column_description))
        {
            if (!column->isCollationSupported())
                throw Exception(ErrorCodes::BAD_COLLATION, "Collations could be specified only for String, LowCardinality(String), "
                                "Nullable(String) or for Array or Tuple, containing them.");
        }

        result.emplace_back(ColumnWithSortDescription{column, sort_column_description, isColumnConst(*column)});
    }

    return result;
}

void getBlockSortPermutationImpl(const Block & block, const SortDescription & description, IColumn::PermutationSortStability stability, UInt64 limit, IColumn::Permutation & permutation)
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

    if (unlikely(all_const))
        return;

    /// If only one column to sort by
    if (columns_with_sort_descriptions.size() == 1)
    {
        auto & column_with_sort_description = columns_with_sort_descriptions[0];

        IColumn::PermutationSortDirection direction = column_with_sort_description.description.direction == -1 ? IColumn::PermutationSortDirection::Descending : IColumn::PermutationSortDirection::Ascending;
        int nan_direction_hint = column_with_sort_description.description.nulls_direction;
        const auto & column = column_with_sort_description.column;

        if (isCollationRequired(column_with_sort_description.description))
            column->getPermutationWithCollation(
                *column_with_sort_description.description.collator, direction, stability, limit, nan_direction_hint, permutation);
        else
            column->getPermutation(direction, stability, limit, nan_direction_hint, permutation);
    }
    else
    {
        size_t size = block.rows();
        permutation.resize(size);
        iota(permutation.data(), size, IColumn::Permutation::value_type(0));

        if (limit >= size)
            limit = 0;

        EqualRanges ranges;
        ranges.emplace_back(0, permutation.size());

        for (const auto & column_with_sort_description : columns_with_sort_descriptions)
        {
            while (!ranges.empty() && limit && limit <= ranges.back().from)
                ranges.pop_back();

            if (ranges.empty())
                break;

            if (column_with_sort_description.column_const)
                continue;

            bool is_collation_required = isCollationRequired(column_with_sort_description.description);
            IColumn::PermutationSortDirection direction = column_with_sort_description.description.direction == -1 ? IColumn::PermutationSortDirection::Descending : IColumn::PermutationSortDirection::Ascending;
            int nan_direction_hint = column_with_sort_description.description.nulls_direction;
            const auto & column = column_with_sort_description.column;

            if (is_collation_required)
            {
                column->updatePermutationWithCollation(
                    *column_with_sort_description.description.collator, direction, stability, limit, nan_direction_hint, permutation, ranges);
            }
            else
            {
                column->updatePermutation(direction, stability, limit, nan_direction_hint, permutation, ranges);
            }
        }
    }
}

bool isIdentityPermutation(const IColumn::Permutation & permutation, size_t limit)
{
    static_assert(sizeof(permutation[0]) == sizeof(UInt64), "Invalid permutation value size");

    size_t permutation_size = permutation.size();
    size_t size = limit == 0 ? permutation_size : std::min(limit, permutation_size);
    if (size == 0)
        return true;

    if (permutation[0] != 0)
        return false;

    size_t i = 0;

#if defined(__SSE2__)
    if (size >= 8)
    {
        static constexpr UInt64 compare_all_elements_equal_mask = (1UL << 16) - 1;

        __m128i permutation_add_vector = { 8, 8 };
        __m128i permutation_compare_values_vectors[4] { { 0, 1 }, { 2, 3 }, { 4, 5 }, { 6, 7 } };

        const size_t * permutation_data = permutation.data();

        static constexpr size_t unroll_count = 8;
        size_t size_unrolled = (size / unroll_count) * unroll_count;

        for (; i < size_unrolled; i += 8)
        {
            UInt64 permutation_equals_vector_mask = compare_all_elements_equal_mask;

            for (size_t j = 0; j < 4; ++j)
            {
                __m128i permutation_data_vector = _mm_loadu_si128(reinterpret_cast<const __m128i *>(permutation_data + i + j * 2));
                __m128i permutation_equals_vector = _mm_cmpeq_epi8(permutation_data_vector, permutation_compare_values_vectors[j]);
                permutation_compare_values_vectors[j] = _mm_add_epi64(permutation_compare_values_vectors[j], permutation_add_vector);
                permutation_equals_vector_mask &= _mm_movemask_epi8(permutation_equals_vector);
            }

            if (permutation_equals_vector_mask != compare_all_elements_equal_mask)
                return false;
        }
    }
#endif

    i = std::max(i, static_cast<size_t>(1));
    for (; i < size; ++i)
        if (permutation[i] != (permutation[i - 1] + 1))
            return false;

    return true;
}

template <typename Comparator>
bool isAlreadySortedImpl(size_t rows, Comparator compare)
{
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

            if (compare(curr_position, prev_position))
                return false;
        }
    }

    for (size_t i = 1; i < rows; ++i)
        if (compare(i, i - 1))
            return false;

    return true;
}

}

void sortBlock(Block & block, const SortDescription & description, UInt64 limit)
{
    IColumn::Permutation permutation;
    getBlockSortPermutationImpl(block, description, IColumn::PermutationSortStability::Unstable, limit, permutation);

    if (permutation.empty())
        return;

    bool is_identity_permutation = isIdentityPermutation(permutation, limit);
    if (is_identity_permutation && limit == 0)
        return;

    size_t columns = block.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        auto & column_to_sort = block.getByPosition(i).column;
        if (is_identity_permutation)
            column_to_sort = column_to_sort->cut(0, std::min(static_cast<size_t>(limit), permutation.size()));
        else
            column_to_sort = column_to_sort->permute(permutation, limit);
    }
}

void stableGetPermutation(const Block & block, const SortDescription & description, IColumn::Permutation & out_permutation)
{
    if (!block)
        return;

    getBlockSortPermutationImpl(block, description, IColumn::PermutationSortStability::Stable, 0, out_permutation);
}

bool isAlreadySorted(const Block & block, const SortDescription & description)
{
    if (!block)
        return true;

    ColumnsWithSortDescriptions columns_with_sort_desc = getColumnsWithSortDescription(block, description);
    bool is_collation_required = false;

    for (auto & column_with_sort_desc : columns_with_sort_desc)
    {
        if (isCollationRequired(column_with_sort_desc.description))
        {
            is_collation_required = true;
            break;
        }
    }

    size_t rows = block.rows();

    if (is_collation_required)
    {
        PartialSortingLessWithCollation less(columns_with_sort_desc);
        return isAlreadySortedImpl(rows, less);
    }

    PartialSortingLess less(columns_with_sort_desc);
    return isAlreadySortedImpl(rows, less);
}

}
