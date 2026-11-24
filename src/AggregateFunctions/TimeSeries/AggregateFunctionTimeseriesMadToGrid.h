#pragma once

#include <cstddef>
#include <optional>
#include <utility>

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
        /// The end of the bucket of `one_bucket_result`, only to check that `removeBefore` drops a bucket out of the window.
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
                /// The bucket of the previous grid point is out of the window, and the bucket of this one is added after.
                chassert(!one_bucket_result || one_bucket_end <= cut_off);
                one_bucket_result.reset();
                return;
            }
            sliding_sum.removeBefore(cut_off);
        }

        [[nodiscard]] std::optional<ResultType> getResult(GridScaleTimestampType /*grid_timestamp*/) const
        {
            if (one_bucket_windows)
                return one_bucket_result;

            const auto & values = sliding_sum.getCurrentSum().values;
            if (values.empty())
                return std::nullopt;
            return calculateTimeseriesMadOfSorted(values);
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
