#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>

#include <Common/NaNUtils.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/sort.h>
#include <base/types.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSortedValues.h>


namespace DB
{

/// R-7 (quantileExactInclusive) quantile of the values sorted by `AggregateFunctionTimeseriesSortedValues::less`, with the
/// Prometheus edge cases of the level: NaN gives NaN, a level below 0 gives -Inf and a level above 1 gives +Inf. There is
/// no result if there are no values. Only the positions it reads must hold their sorted values.
template <typename ValueType>
std::optional<Float64> calculateTimeseriesQuantileOfSorted(const VectorWithMemoryTracking<ValueType> & values, Float64 phi)
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

/// The quantile of unsorted values: only the values `calculateTimeseriesQuantileOfSorted` reads are put in their sorted
/// positions, in O(n) instead of sorting. Reorders the values.
template <typename ValueType>
std::optional<Float64> calculateTimeseriesQuantile(VectorWithMemoryTracking<ValueType> & values, Float64 phi)
{
    if (!values.empty() && phi >= 0.0 && phi <= 1.0)
    {
        /// The same rank as in `calculateTimeseriesQuantileOfSorted`.
        const Float64 rank = phi * static_cast<Float64>(values.size() - 1);
        const auto lower = values.begin() + static_cast<size_t>(std::floor(rank));
        const auto less = AggregateFunctionTimeseriesSortedValues<ValueType>::less;
        ::nth_element(values.begin(), lower, values.end(), less);
        if (std::ceil(rank) != std::floor(rank))
            std::iter_swap(lower + 1, std::min_element(lower + 1, values.end(), less));
    }
    return calculateTimeseriesQuantileOfSorted(values, phi);
}

/// The median absolute deviation `median(|x - median(x)|)` of non-empty unsorted values, both medians taken like the
/// 0.5-quantile of `calculateTimeseriesQuantileOfSorted` and selected instead of sorting. Like in Prometheus, it is NaN if
/// a value is NaN or the median is not finite.
template <typename ValueType>
Float64 calculateTimeseriesMad(VectorWithMemoryTracking<ValueType> values)
{
    if (std::any_of(values.begin(), values.end(), [](ValueType value) { return isNaN(value); }))
        return std::numeric_limits<Float64>::quiet_NaN();

    const Float64 median = *calculateTimeseriesQuantile(values, 0.5);
    if (!std::isfinite(median))
        return std::numeric_limits<Float64>::quiet_NaN();

    /// The middle deviation, or the two middle ones weighted by 0.5. No deviation is NaN, so `std::less` orders them.
    auto median_of_deviations = [](auto & deviations) -> Float64
    {
        const size_t middle = (deviations.size() - 1) / 2;
        ::nth_element(deviations.begin(), deviations.begin() + middle, deviations.end());
        if (deviations.size() % 2 == 1)
            return deviations[middle];
        return deviations[middle] * 0.5 + *std::min_element(deviations.begin() + middle + 1, deviations.end()) * 0.5;
    };

    /// `Float64` deviations, as in `calculateTimeseriesKthSmallestDeviation`: in place for `Float64` values, in a buffer
    /// for `Float32`.
    if constexpr (std::is_same_v<ValueType, Float64>)
    {
        for (Float64 & value : values)
            value = std::abs(value - median);
        return median_of_deviations(values);
    }
    else
    {
        VectorWithMemoryTracking<Float64> deviations(values.size());
        for (size_t i = 0; i < values.size(); ++i)
            deviations[i] = std::abs(static_cast<Float64>(values[i]) - median);
        return median_of_deviations(deviations);
    }
}

/// The k-th smallest (counting from 0) of the deviations `|values[i] - median|`, where `values` are `count` real values
/// in ascending order, `median` is finite and `k < count`. If `with_next`, the (k + 1)-th smallest deviation is returned
/// as well, which requires `k + 1 < count`.
template <bool with_next, typename ValueType>
std::conditional_t<with_next, std::pair<Float64, Float64>, Float64>
calculateTimeseriesKthSmallestDeviation(const ValueType * values, size_t count, Float64 median, size_t k)
{
    constexpr Float64 infinity = std::numeric_limits<Float64>::infinity();

    /// The k + 1 values closest to the median are consecutive in `values`, so the k-th deviation is the smallest
    /// `max(deviation_of_first(begin), deviation_of_last(begin))` over the start `begin` of k + 1 consecutive values.
    auto deviation_of_first = [&](size_t begin) { return median - static_cast<Float64>(values[begin]); };
    auto deviation_of_last = [&](size_t begin) { return static_cast<Float64>(values[begin + k]) - median; };

    /// The first deviation does not grow with `begin` and the second one does not shrink, so the minimum is at the first
    /// `begin` where the second one is not less than the first one, or right before it.
    size_t low = 0;
    size_t high = count - k - 1;
    while (low < high)
    {
        const size_t middle = low + (high - low) / 2;
        if (deviation_of_first(middle) <= deviation_of_last(middle))
            high = middle;
        else
            low = middle + 1;
    }

    const Float64 kth_deviation_at_low = std::max(deviation_of_first(low), deviation_of_last(low));
    const Float64 kth_deviation_before_low = low > 0 ? std::max(deviation_of_first(low - 1), deviation_of_last(low - 1)) : infinity;
    /// `abs` turns `-0.0` (`median - value` for zeros) into `+0.0`, as `|value - median|` in Prometheus.
    const Float64 kth_deviation = std::abs(std::min(kth_deviation_at_low, kth_deviation_before_low));

    if constexpr (!with_next)
        return kth_deviation;
    else
    {
        /// The (k + 1)-th deviation is the deviation of the closer one of the two values next to the k + 1 closest values.
        const size_t begin = kth_deviation_before_low < kth_deviation_at_low ? low - 1 : low;
        auto deviation_at = [&](size_t index) { return std::abs(static_cast<Float64>(values[index]) - median); };
        const Float64 next_deviation = std::min(
            begin > 0 ? deviation_at(begin - 1) : infinity,
            begin + k + 1 < count ? deviation_at(begin + k + 1) : infinity);
        return std::make_pair(kth_deviation, next_deviation);
    }
}

}
