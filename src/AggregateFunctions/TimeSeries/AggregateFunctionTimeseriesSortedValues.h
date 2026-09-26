#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <optional>
#include <utility>

#include <Common/Exception.h>
#include <Common/NaNUtils.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/sort.h>
#include <base/types.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// The values of one bucket, or of the whole window, sorted by `less`: the `Summary` of the aggregate functions reading
/// their result from the sorted values of the window (`timeSeriesQuantileToGrid`, `timeSeriesMadToGrid`).
/// Merging two summaries is a merge of two sorted runs, and a merged summary can be taken out again by a pass over both
/// sorted runs, so the `SlidingSum` keeps one running summary of the window and the result is read from it without sorting.
template <typename ValueType>
struct AggregateFunctionTimeseriesSortedValues
{
    VectorWithMemoryTracking<ValueType> values;

    /// The order of the values: NaN samples are kept and ordered before every real value, like in Prometheus.
    /// It is a strict weak ordering (all NaNs are equivalent), which plain `<` on floats is not.
    static bool less(ValueType lhs, ValueType rhs)
    {
        if (isNaN(lhs))
            return !isNaN(rhs);
        return !isNaN(rhs) && lhs < rhs;
    }

    /// Adds values in any order.
    void add(VectorWithMemoryTracking<ValueType> && new_values)
    {
        ::sort(new_values.begin(), new_values.end(), less);
        if (values.empty())
        {
            values = std::move(new_values);
            return;
        }

        const size_t old_size = values.size();
        values.insert(values.end(), new_values.begin(), new_values.end());
        std::inplace_merge(values.begin(), values.begin() + old_size, values.end(), less);
    }

    void merge(const AggregateFunctionTimeseriesSortedValues & other)
    {
        if (other.values.empty())
            return;

        const size_t old_size = values.size();
        values.insert(values.end(), other.values.begin(), other.values.end());
        std::inplace_merge(values.begin(), values.begin() + old_size, values.end(), less);
    }

    /// Removes the values of `leaving`, which was merged before: every value of `leaving` drops one equivalent value.
    void unmerge(const AggregateFunctionTimeseriesSortedValues & leaving, const AggregateFunctionTimeseriesSortedValues * /*new_first*/)
    {
        size_t kept = 0;
        size_t leaving_index = 0;
        for (size_t i = 0; i < values.size(); ++i)
        {
            if (leaving_index < leaving.values.size() && !less(values[i], leaving.values[leaving_index]))
            {
                ++leaving_index;
                continue;
            }
            values[kept++] = values[i];
        }

        if (leaving_index != leaving.values.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot remove values from the sorted values of a time series window: they were not added");

        values.resize(kept);
    }

    /// R-7 (quantileExactInclusive) quantile of the values, with the Prometheus edge cases of the level: NaN gives NaN,
    /// a level below 0 gives -Inf and a level above 1 gives +Inf. There is no result if there are no values.
    std::optional<Float64> quantile(Float64 phi) const
    {
        if (values.empty())
            return std::nullopt;

        if (std::isnan(phi))
            return std::numeric_limits<Float64>::quiet_NaN();
        if (phi < 0.0)
            return -std::numeric_limits<Float64>::infinity();
        if (phi > 1.0)
            return std::numeric_limits<Float64>::infinity();

        const size_t n = values.size();
        if (n == 1)
            return static_cast<Float64>(values[0]);

        /// rank = phi * (n - 1), interpolated between the neighbouring values.
        const Float64 rank = phi * static_cast<Float64>(n - 1);
        const size_t lower = static_cast<size_t>(std::floor(rank));
        const size_t upper = static_cast<size_t>(std::ceil(rank));

        /// An exact rank gives the sample itself. Prometheus has no such shortcut and computes `lower * 1 + upper * 0`,
        /// which is NaN when either sample is infinite.
        if (lower == upper)
            return static_cast<Float64>(values[lower]);

        /// The weighted form of Prometheus (`lower * (1 - weight) + upper * weight`) rather than `lower + weight * (upper - lower)`,
        /// so that infinite samples give the infinity instead of `Inf - Inf = NaN`.
        const Float64 weight = rank - static_cast<Float64>(lower);
        return static_cast<Float64>(values[lower]) * (1.0 - weight) + static_cast<Float64>(values[upper]) * weight;
    }
};

}
