#pragma once

#include <cmath>
#include <cstddef>
#include <limits>
#include <optional>
#include <utility>

#include <Common/NaNUtils.h>
#include <Common/VectorWithMemoryTracking.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesQuantileHelpers.h>
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

    /// Sliding aggregator: keeps the sorted values of the window and computes their median absolute deviation, or only the
    /// result if `one_bucket_windows`.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<Summary> sliding_sum;

        static_assert(decltype(sliding_sum)::is_invertible);

        /// Each window is one bucket (the window is not longer than the step, or the grid start equals the end), read by one
        /// grid point only: `add` computes the result from it unsorted, and only the result is kept.
        bool one_bucket_windows;
        std::optional<ResultType> one_bucket_result;
        GridScaleTimestampType one_bucket_end{};

        explicit Aggregator(bool one_bucket_windows_)
            : one_bucket_windows(one_bucket_windows_)
        {
        }

        void add(const Samples & samples, GridScaleTimestampType bucket_end_timestamp)
        {
            VectorWithMemoryTracking<ValueType> values;
            samples.forEachSample([&values](TimestampType /*timestamp*/, ValueType value)
            {
                values.push_back(value);
            });

            if (values.empty())
                return;

            if (one_bucket_windows)
            {
                /// Replaces the result of the previous grid point, which is out of this window.
                one_bucket_result = calculateTimeseriesMad(std::move(values));
                one_bucket_end = bucket_end_timestamp;
                return;
            }

            Summary summary;
            summary.add(std::move(values));
            sliding_sum.add(std::move(summary), bucket_end_timestamp);
        }

        void removeBefore(GridScaleTimestampType cut_off)
        {
            if (one_bucket_windows)
            {
                if (one_bucket_end <= cut_off)
                    one_bucket_result.reset();
                return;
            }
            sliding_sum.removeBefore(cut_off);
        }

        [[nodiscard]] std::optional<ResultType> getResult(GridScaleTimestampType /*grid_timestamp*/) const
        {
            if (one_bucket_windows)
                return one_bucket_result;

            const Summary & summary = sliding_sum.getCurrentSum();
            const auto & values = summary.values;
            if (values.empty())
                return std::nullopt;

            /// Like in Prometheus, a NaN sample in the window makes the result NaN. The NaN samples sort first.
            if (isNaN(values.front()))
                return std::numeric_limits<Float64>::quiet_NaN();

            const Float64 median = *calculateTimeseriesQuantileOfSorted(values, 0.5);

            /// A median interpolated between -Inf and +Inf is NaN. An infinite median makes the deviations of the samples
            /// equal to it NaN (`Inf - Inf`), and there are always enough of them to cover the median position of the deviations.
            if (!std::isfinite(median))
                return std::numeric_limits<Float64>::quiet_NaN();

            /// The median of the deviations is taken like the median of the values: the middle deviation for an odd count,
            /// the two middle deviations weighted by 0.5 each for an even count.
            const size_t count = values.size();
            const size_t middle = (count - 1) / 2;
            if (count % 2 == 1)
                return calculateTimeseriesKthSmallestDeviation</* with_next = */ false>(values.data(), count, median, middle);

            const auto [lower_deviation, upper_deviation] = calculateTimeseriesKthSmallestDeviation</* with_next = */ true>(values.data(), count, median, middle);
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
        return Aggregator{Base::buckets_per_window == 1};
    }
};

}
