#include <Interpreters/sortBlock.h>

#include <algorithm>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnTuple.h>
#include <Columns/findEqualRangeEndAssumeSorted.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Functions/FunctionHelpers.h>
#include <base/sort.h>
#include <Common/BitHelpers.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/PODArray.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/iota.h>

namespace CurrentMetrics
{
    extern const Metric SortBlockInParallelThreads;
    extern const Metric SortBlockInParallelThreadsActive;
    extern const Metric SortBlockInParallelThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_COLLATION;
#ifndef NDEBUG
    extern const int LOGICAL_ERROR;
#endif
}

/// Column with description for sort
struct ColumnWithSortDescription
{
    ColumnPtr column = nullptr;
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

        auto column = block.getColumnOrSubcolumnByName(sort_column_description.column_name);

        if (isCollationRequired(sort_column_description))
        {
            if (!column.column->isCollationSupported())
                throw Exception(ErrorCodes::BAD_COLLATION, "Collations could be specified only for String, LowCardinality(String), "
                                "Nullable(String) or for Array or Tuple, containing them.");
        }

        result.emplace_back(ColumnWithSortDescription{column.column, sort_column_description, isColumnConst(*column.column)});
    }

    return result;
}

/// Sorts each of `ranges` of `permutation` by the columns in order; `ranges` keeps the rows that are still equal.
void updatePermutationByColumns(
    const ColumnsWithSortDescriptions & columns_with_sort_descriptions,
    IColumn::PermutationSortStability stability,
    UInt64 limit,
    IColumn::Permutation & permutation,
    EqualRanges & ranges)
{
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

#ifndef NDEBUG
        /// updatePermutation must keep `equal_ranges` sorted in ascending order of `from`; the limit
        /// shortcuts above (and IColumn::updatePermutationImpl) rely on it. Catch violators early.
        chassert(std::ranges::is_sorted(ranges, {}, &EqualRange::from),
            "updatePermutation returned equal_ranges not sorted by `from`");
#endif
    }
}

/// `equal_ranges`, when requested, receives the ranges (of two or more positions of the permutation) whose
/// rows compare equal on the whole description; only supported for a full sort (no limit).
void getBlockSortPermutationImpl(
    const Block & block,
    const SortDescription & description,
    IColumn::PermutationSortStability stability,
    UInt64 limit,
    IColumn::Permutation & permutation,
    EqualRanges * equal_ranges = nullptr)
{
    chassert(!equal_ranges || limit == 0);

    if (block.empty())
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

        /// The single-column sort does not track the equal ranges: the galloping search finds them in the
        /// sorted order of the permutation, cheaply both for the long ranges of a skewed input and for the
        /// singleton ranges of a distinct one. Every probe is a virtual comparison, so the linear probe is
        /// kept short (as in `IColumn::getEqualRangeEndAssumeSorted`); the comparison honors the collation.
        if (equal_ranges)
        {
            static constexpr size_t linear_probe = 8;
            PartialSortingLessWithCollation compare(columns_with_sort_descriptions);
            const size_t size = permutation.size();
            for (size_t begin = 0; begin < size;)
            {
                const size_t range_end = findEqualRangeEndAssumeSorted(
                    begin, size, linear_probe, [&](size_t pos) { return compare.compare(permutation[begin], permutation[pos]) == 0; });
                if (range_end - begin > 1)
                    equal_ranges->emplace_back(begin, range_end);
                begin = range_end;
            }
        }
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

        updatePermutationByColumns(columns_with_sort_descriptions, stability, limit, permutation, ranges);

        if (equal_ranges)
            *equal_ranges = std::move(ranges);
    }
}

}

bool isIdentityPermutation(const IColumn::Permutation & permutation, size_t limit)
{
    static_assert(sizeof(permutation[0]) == sizeof(UInt64), "Invalid permutation value size");

    const size_t permutation_size = permutation.size();
    const size_t size = limit == 0 ? permutation_size : std::min(limit, permutation_size);

    /// The identity holds exactly when every element equals its own index. A block is folded into
    /// a single value, so the loop is branchless and vectorizes, and one check per block keeps the
    /// early exit. Elements are compared against the first element of the block rather than their
    /// absolute index, which keeps the vector of offsets loop invariant: with the absolute index
    /// the compiler rebuilds that vector on every block, and on AArch64 that costs more than the
    /// comparison itself.
    static constexpr size_t block_size = 64;

    /// Checked on its own, so a permutation that does not start at zero costs one load.
    if (size != 0 && permutation[0] != 0)
        return false;

    size_t i = 0;
    for (; i + block_size <= size; i += block_size)
    {
        const UInt64 base = permutation[i];
        UInt64 different = base ^ i;

        for (size_t j = 0; j < block_size; ++j)
            different |= (permutation[i + j] - base) ^ j;

        if (different)
            return false;
    }

    for (; i < size; ++i)
    {
        if (permutation[i] != i)
            return false;
    }

    return true;
}

namespace
{

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

#ifndef NDEBUG
template <typename Comparator>
void checkSortedWithPermutationImpl(size_t rows, Comparator compare, UInt64 limit, const IColumn::Permutation & permutation)
{
    if (limit && limit < rows)
        rows = limit;

    const bool no_permutaiton = permutation.empty();

    for (size_t i = 1; i < rows; ++i)
    {
        const size_t current_row = no_permutaiton ? i : permutation[i];
        const size_t previous_row = no_permutaiton ? (i - 1) : permutation[i - 1];

        if (compare(current_row, previous_row))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Rows are not sorted with permutation, position {}, previous_row index {}, current_row index {}", i, previous_row, current_row);
    }
}

void checkSortedWithPermutation(const Block & block, const SortDescription & description, UInt64 limit, const IColumn::Permutation & permutation)
{
    if (block.empty())
        return;

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
        checkSortedWithPermutationImpl(rows, less, limit, permutation);
        return;
    }
    else
    {
        PartialSortingLess less(columns_with_sort_desc);
        checkSortedWithPermutationImpl(rows, less, limit, permutation);
        return;
    }
}
#endif

}

size_t estimateSortingWorkspace(size_t rows)
{
    /// A sorting permutation stores each row's original index. Include array padding and the
    /// power-of-two capacity rounding, rather than counting only the indices themselves.
    using Permutation = IColumn::Permutation;
    const size_t permutation_bytes = roundUpToPowerOfTwoOrZero(PODArrayDetails::minimum_memory_for_elements(
        rows, sizeof(Permutation::value_type), Permutation::pad_left, Permutation::pad_right));

    /// Numeric radix sorting holds two value-index arrays alongside the permutation. Each pair can
    /// occupy twice an index's size, and the histograms need up to 8 KiB. This allowance also covers
    /// the equal-key ranges used by comparison sorting and duplicate removal.
    return 5 * permutation_bytes + (8 << 10);
}

namespace
{

/// Reorders the rows of `block` by a non-empty sorting `permutation`, keeping the first `limit` rows if `limit` != 0.
void applySortPermutation(Block & block, const IColumn::Permutation & permutation, UInt64 limit)
{
    bool is_identity_permutation = isIdentityPermutation(permutation, limit);
    if (is_identity_permutation && limit == 0)
        return;

    size_t output_rows = limit ? std::min(static_cast<size_t>(limit), permutation.size()) : permutation.size();
    Columns columns = block.getColumns();
    transformColumnsWithSharedIndex(
        columns,
        [&](const ColumnPtr & col) { return is_identity_permutation ? col->cut(0, output_rows) : col->permute(permutation, limit); });
    block.setColumns(columns);
}

}

void sortBlock(Block & block, const SortDescription & description, UInt64 limit, IColumn::PermutationSortStability stability)
{
    IColumn::Permutation permutation;

#ifndef NDEBUG
    block.checkNumberOfRows();
#endif
    getBlockSortPermutationImpl(block, description, stability, limit, permutation);

#ifndef NDEBUG
    checkSortedWithPermutation(block, description, limit, permutation);
#endif

    if (permutation.empty())
        return;

    applySortPermutation(block, permutation, limit);
}

void sortBlockInParallel(Block & block, const SortDescription & description, size_t max_threads)
{
    static constexpr size_t min_rows_per_range = 65536;
    static constexpr size_t samples_per_range = 32;

    const size_t rows = block.rows();
    const size_t num_ranges = std::min(max_threads, rows / min_rows_per_range);
    if (num_ranges < 2)
    {
        sortBlock(block, description);
        return;
    }

#ifndef NDEBUG
    block.checkNumberOfRows();
#endif

    const ColumnsWithSortDescriptions columns = getColumnsWithSortDescription(block, description);
    if (std::ranges::all_of(columns, &ColumnWithSortDescription::column_const))
        return;

    const PartialSortingLessWithCollation less(columns);

    /// A sample sort: range `r` gets the rows not less than splitter `r - 1` and less than splitter `r`,
    /// so equal rows share a range and the sorted ranges, concatenated, are the sorted block.
    std::vector<size_t> samples(samples_per_range * num_ranges);
    for (size_t i = 0; i < samples.size(); ++i)
        samples[i] = i * rows / samples.size();
    ::sort(samples.begin(), samples.end(), less);

    std::vector<size_t> splitters(num_ranges - 1);
    for (size_t range = 0; range + 1 < num_ranges; ++range)
        splitters[range] = samples[(range + 1) * samples_per_range - 1];

    /// Rows are classified and scattered in `num_ranges` contiguous chunks. `offsets[chunk * num_ranges + range]`
    /// counts the rows of `chunk` in `range`, then holds the position where the next of them goes.
    PaddedPODArray<UInt32> range_of_row(rows);
    std::vector<size_t> offsets(num_ranges * num_ranges);
    std::vector<size_t> range_begin(num_ranges + 1);
    IColumn::Permutation permutation(rows);

    auto chunk_begin = [&](size_t chunk) { return chunk * rows / num_ranges; };

    auto classify_chunk = [&](size_t chunk)
    {
        size_t * counts = &offsets[chunk * num_ranges];
        for (size_t row = chunk_begin(chunk); row < chunk_begin(chunk + 1); ++row)
        {
            const size_t range = std::upper_bound(splitters.begin(), splitters.end(), row, less) - splitters.begin();
            range_of_row[row] = static_cast<UInt32>(range);
            ++counts[range];
        }
    };

    auto scatter_chunk = [&](size_t chunk)
    {
        size_t * next_position = &offsets[chunk * num_ranges];
        for (size_t row = chunk_begin(chunk); row < chunk_begin(chunk + 1); ++row)
            permutation[next_position[range_of_row[row]]++] = row;
    };

    auto sort_range = [&](size_t range)
    {
        if (range_begin[range + 1] - range_begin[range] < 2)
            return;

        EqualRanges equal_ranges;
        equal_ranges.emplace_back(range_begin[range], range_begin[range + 1]);
        updatePermutationByColumns(columns, IColumn::PermutationSortStability::Unstable, /*limit=*/ 0, permutation, equal_ranges);
    };

    /// Declared after everything its jobs use: its destructor joins the jobs that are still running when scheduling throws.
    ThreadPool pool(
        CurrentMetrics::SortBlockInParallelThreads,
        CurrentMetrics::SortBlockInParallelThreadsActive,
        CurrentMetrics::SortBlockInParallelThreadsScheduled,
        num_ranges);

    auto run_in_parallel = [&](const auto & job)
    {
        for (size_t i = 0; i < num_ranges; ++i)
        {
            pool.scheduleOrThrowOnError(
                [&job, i, thread_group = CurrentThread::getGroup()]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::SORT_BLOCK);
                    job(i);
                });
        }
        pool.wait();
    };

    run_in_parallel(classify_chunk);

    size_t position = 0;
    for (size_t range = 0; range < num_ranges; ++range)
    {
        range_begin[range] = position;
        for (size_t chunk = 0; chunk < num_ranges; ++chunk)
        {
            size_t & offset = offsets[chunk * num_ranges + range];
            const size_t count = offset;
            offset = position;
            position += count;
        }
    }
    range_begin[num_ranges] = position;

    run_in_parallel(scatter_chunk);
    run_in_parallel(sort_range);

#ifndef NDEBUG
    checkSortedWithPermutation(block, description, /*limit=*/ 0, permutation);
#endif

    applySortPermutation(block, permutation, /*limit=*/ 0);
}

void sortBlockAndDeduplicate(Block & block, const SortDescription & description, IColumn::PermutationSortStability stability)
{
#ifndef NDEBUG
    block.checkNumberOfRows();
#endif
    IColumn::Permutation permutation;
    EqualRanges equal_ranges;
    getBlockSortPermutationImpl(block, description, stability, /*limit=*/ 0, permutation, &equal_ranges);

#ifndef NDEBUG
    checkSortedWithPermutation(block, description, /*limit=*/ 0, permutation);
#endif

    Columns columns = block.getColumns();

    /// No non-constant sort column: every row is equal to the first one.
    if (permutation.empty())
    {
        if (block.rows() > 1)
        {
            transformColumnsWithSharedIndex(columns, [](const ColumnPtr & col) { return col->cut(0, 1); });
            block.setColumns(columns);
        }
        return;
    }

    if (!equal_ranges.empty())
    {
        /// Keep the first position of each equal range. The write position never passes the read
        /// position, so retained entries can be compacted in the existing permutation.
        size_t read_pos = 0;
        size_t write_pos = 0;
        for (const auto & range : equal_ranges)
        {
            while (read_pos <= range.from)
                permutation[write_pos++] = permutation[read_pos++];
            read_pos = range.to;
        }
        while (read_pos < permutation.size())
            permutation[write_pos++] = permutation[read_pos++];
        permutation.resize(write_pos);
    }
    else if (isIdentityPermutation(permutation, /*limit=*/ 0))
        return;

    transformColumnsWithSharedIndex(columns, [&](const ColumnPtr & col) { return col->permute(permutation, permutation.size()); });
    block.setColumns(columns);
}

void stableGetPermutation(const Block & block, const SortDescription & description, IColumn::Permutation & out_permutation)
{
    if (block.empty())
        return;

    getBlockSortPermutationImpl(block, description, IColumn::PermutationSortStability::Stable, 0, out_permutation);

#ifndef NDEBUG
    checkSortedWithPermutation(block, description, 0, out_permutation);
#endif
}

bool isAlreadySorted(const Block & block, const SortDescription & description)
{
    if (block.empty())
        return true;

    ColumnsWithSortDescriptions columns_with_sort_desc = getColumnsWithSortDescription(block, description);
    bool is_collation_required = false;
    bool all_const = true;

    for (const auto & column_with_sort_desc : columns_with_sort_desc)
    {
        all_const = all_const && column_with_sort_desc.column_const;
        if (!is_collation_required && isCollationRequired(column_with_sort_desc.description))
            is_collation_required = true;
    }

    if (unlikely(all_const))
        return true;

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
