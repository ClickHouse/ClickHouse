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

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSortedValues.h>


namespace DB
{

/// Traits of timeSeriesMadToGrid (PromQL mad_over_time): the median absolute deviation `median(|x - median(x)|)` of the
/// values in the window, both medians taken the way timeSeriesQuantileToGrid takes the 0.5-quantile.
template <typename TimestampType_, typename ValueType_>
struct AggregateFunctionTimeseriesMadToGridTraits
{
    using GridScaleTimestampType = DateTime64;
    using TimestampType = TimestampType_;
    using ValueType = ValueType_;

    /// The medians are interpolated between the samples, so the result is calculated with double precision.
    using ResultType = Float64;

    static String getName()
    {
        return "timeSeriesMadToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// The bucket stores raw samples: the timestamps are needed to collapse duplicate timestamps into one sample.
    using Bucket = Samples;

    /// The sorted values of the window, so the median is read from them without sorting.
    using Summary = AggregateFunctionTimeseriesSortedValues<ValueType>;

    /// The k-th smallest (counting from 0) of the deviations `|values[i] - median|`, where `values` are `count` real values
    /// in ascending order, `median` is finite and `k < count`. If `with_next`, the (k + 1)-th smallest deviation is returned
    /// as well, which requires `k + 1 < count`.
    template <bool with_next>
    static std::conditional_t<with_next, std::pair<Float64, Float64>, Float64>
    kthSmallestDeviation(const ValueType * values, size_t count, Float64 median, size_t k)
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
        const Float64 kth_deviation = std::min(kth_deviation_at_low, kth_deviation_before_low);

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

    /// Sliding aggregator: keeps the sorted values of the window and computes the median absolute deviation from them.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<Summary> sliding_sum;

        static_assert(decltype(sliding_sum)::is_invertible);

        void add(const Samples & samples, GridScaleTimestampType bucket_end_timestamp)
        {
            VectorWithMemoryTracking<ValueType> values;
            samples.forEachSample([&values](TimestampType /*timestamp*/, ValueType value)
            {
                values.push_back(value);
            });

            if (values.empty())
                return;

            Summary summary;
            summary.add(std::move(values));
            sliding_sum.add(std::move(summary), bucket_end_timestamp);
        }

        void removeBefore(GridScaleTimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<ResultType> getResult(GridScaleTimestampType /*grid_timestamp*/) const
        {
            const Summary & summary = sliding_sum.getCurrentSum();
            const auto & values = summary.values;
            if (values.empty())
                return std::nullopt;

            /// Like in Prometheus, a NaN sample in the window makes the result NaN. The NaN samples sort first.
            if (isNaN(values.front()))
                return std::numeric_limits<Float64>::quiet_NaN();

            const Float64 median = *summary.quantile(0.5);

            /// A median interpolated between -Inf and +Inf is NaN. An infinite median makes the deviations of the samples
            /// equal to it NaN (`Inf - Inf`), and there are always enough of them to cover the median position of the deviations.
            if (!std::isfinite(median))
                return std::numeric_limits<Float64>::quiet_NaN();

            /// The median of the deviations is taken like the median of the values: the middle deviation for an odd count,
            /// the two middle deviations weighted by 0.5 each for an even count.
            const size_t count = values.size();
            const size_t middle = (count - 1) / 2;
            if (count % 2 == 1)
                return kthSmallestDeviation</* with_next = */ false>(values.data(), count, median, middle);

            const auto [lower_deviation, upper_deviation] = kthSmallestDeviation</* with_next = */ true>(values.data(), count, median, middle);
            return lower_deviation * 0.5 + upper_deviation * 0.5;
        }
    };

    static constexpr UInt16 FORMAT_VERSION = 1;
};


/// Aggregate function that computes the median absolute deviation of time series values on a regular time grid.
/// For each grid point the result is `median(|x - median(x)|)` over the sample values within the grid point's window,
/// with both medians taken by the R-7 (inclusive) method like in Prometheus.
template <typename TimestampType_, typename ValueType_>
class AggregateFunctionTimeseriesMadToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesMadToGrid<TimestampType_, ValueType_>,
        AggregateFunctionTimeseriesMadToGridTraits<TimestampType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesMadToGridTraits<TimestampType_, ValueType_>;

    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesMadToGrid, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return {};
    }
};

}
