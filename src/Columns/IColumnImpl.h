#pragma once
/**
  * This file implements template methods of IColumn that depend on other types
  * we don't want to include.
  * Currently, getPermutationImpl and updatePermutationImpl depend on PODArray
  * implementation.
  */

#include <algorithm>
#include <Columns/IColumn.h>
#include <base/sort.h>
#include <Common/PODArray.h>
#include <Common/iota.h>


namespace DB
{

struct DefaultSort
{
    template <typename RandomIt, typename Compare>
    void operator()(RandomIt begin, RandomIt end, Compare compare)
    {
        ::sort(begin, end, compare);
    }
};

struct DefaultPartialSort
{
    template <typename RandomIt, typename Compare>
    void operator()(RandomIt begin, RandomIt middle, RandomIt end, Compare compare)
    {
        ::partial_sort(begin, middle, end, compare);
    }
};

template <typename ComparatorBase, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability>
struct ComparatorHelperImpl : public ComparatorBase
{
    using Base = ComparatorBase;
    using Base::Base;

    bool operator()(size_t lhs, size_t rhs) const
    {
        int res = Base::compare(lhs, rhs);

        if constexpr (stability == IColumn::PermutationSortStability::Stable)
        {
            if (unlikely(res == 0))
                return lhs < rhs;
        }

        if constexpr (direction == IColumn::PermutationSortDirection::Ascending)
            return res < 0;
        else
            return res > 0;
    }
};

template <typename ComparatorBase>
struct ComparatorEqualHelperImpl : public ComparatorBase
{
    using Base = ComparatorBase;
    using Base::Base;

    bool operator()(size_t lhs, size_t rhs) const
    {
        int res = Base::compare(lhs, rhs);
        return res == 0;
    }
};

template <typename ComparatorBase>
using ComparatorAscendingUnstableImpl = ComparatorHelperImpl<
    ComparatorBase,
    IColumn::PermutationSortDirection::Ascending,
    IColumn::PermutationSortStability::Unstable>;

template <typename ComparatorBase>
using ComparatorAscendingStableImpl = ComparatorHelperImpl<
    ComparatorBase,
    IColumn::PermutationSortDirection::Ascending,
    IColumn::PermutationSortStability::Stable>;

template <typename ComparatorBase>
using ComparatorDescendingUnstableImpl = ComparatorHelperImpl<
    ComparatorBase,
    IColumn::PermutationSortDirection::Descending,
    IColumn::PermutationSortStability::Unstable>;

template <typename ComparatorBase>
using ComparatorDescendingStableImpl = ComparatorHelperImpl<
    ComparatorBase,
    IColumn::PermutationSortDirection::Descending,
    IColumn::PermutationSortStability::Stable>;

template <typename ComparatorBase>
using ComparatorEqualImpl = ComparatorEqualHelperImpl<ComparatorBase>;

template <typename Compare, typename Sort, typename PartialSort>
void IColumn::getPermutationImpl(
    size_t limit,
    Permutation & res,
    Compare compare,
    Sort full_sort,
    PartialSort partial_sort) const
{
    size_t data_size = size();

    if (data_size == 0)
        return;

    res.resize(data_size);

    if (limit >= data_size)
        limit = 0;

    iota(res.data(), data_size, Permutation::value_type(0));

    if (limit)
    {
        partial_sort(res.begin(), res.begin() + limit, res.end(), compare);
        return;
    }

    full_sort(res.begin(), res.end(), compare);
}

template <typename Compare, typename Equals, typename Sort, typename PartialSort>
void IColumn::updatePermutationImpl(
    size_t limit,
    Permutation & res,
    EqualRanges & equal_ranges,
    Compare compare,
    Equals equals,
    Sort full_sort,
    PartialSort partial_sort) const
{
    if (equal_ranges.empty())
        return;

    if (limit >= size() || limit > equal_ranges.back().to)
        limit = 0;

    EqualRanges new_ranges;

    size_t number_of_ranges = equal_ranges.size();
    if (limit)
        --number_of_ranges;

    for (size_t i = 0; i < number_of_ranges; ++i)
    {
        const auto & [first, last] = equal_ranges[i];
        full_sort(res.begin() + first, res.begin() + last, compare);

        size_t new_first = first;
        for (size_t j = first + 1; j < last; ++j)
        {
            if (!equals(res[j], res[new_first]))
            {
                if (j - new_first > 1)
                    new_ranges.emplace_back(new_first, j);

                new_first = j;
            }
        }

        if (last - new_first > 1)
            new_ranges.emplace_back(new_first, last);
    }

    if (limit)
    {
        const auto & [first, last] = equal_ranges.back();

        if (limit < first || limit > last)
        {
            equal_ranges = std::move(new_ranges);
            return;
        }

        /// Since then we are working inside the interval.
        partial_sort(res.begin() + first, res.begin() + limit, res.begin() + last, compare);

        size_t new_first = first;
        for (size_t j = first + 1; j < limit; ++j)
        {
            if (!equals(res[j], res[new_first]))
            {
                if (j - new_first > 1)
                    new_ranges.emplace_back(new_first, j);
                new_first = j;
            }
        }

        size_t new_last = limit;
        for (size_t j = limit; j < last; ++j)
        {
            if (equals(res[j], res[new_first]))
            {
                std::swap(res[j], res[new_last]);
                ++new_last;
            }
        }

        if (new_last - new_first > 1)
            new_ranges.emplace_back(new_first, new_last);
    }

    equal_ranges = std::move(new_ranges);
}

}
