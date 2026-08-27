#pragma once

#include <algorithm>
#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include <Common/VectorWithMemoryTracking.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

template <typename TimestampType_, typename IntervalType_, typename ValueType_>
struct AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return "timeSeriesDoubleExponentialSmoothingToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Per-bucket summary that collects the bucket's (timestamp, value) samples, and (once combined over a window)
    /// the window's samples too. Double exponential smoothing is order-dependent, so the timestamps are kept and
    /// used to order the samples before smoothing. There is no `unmerge`, so the `SlidingSum` recomputes the
    /// window's samples per grid point.
    struct Summary
    {
        VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> samples;

        void merge(const Summary & other)
        {
            samples.insert(samples.end(), other.samples.begin(), other.samples.end());
        }
    };

    /// Sliding aggregator: keeps the window's samples in a `SlidingSum` and computes Prometheus's
    /// `double_exponential_smoothing` (Holt-Winters double exponential smoothing) per grid point.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;
        Float64 smoothing_factor = 0;
        Float64 trend_factor = 0;

        Aggregator(Float64 smoothing_factor_, Float64 trend_factor_)
            : smoothing_factor(smoothing_factor_), trend_factor(trend_factor_)
        {
        }

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            Summary summary;
            samples.forEachSample([&summary](TimestampType timestamp, ValueType value)
            {
                summary.samples.emplace_back(timestamp, value);
            });
            add(std::move(summary), bucket_end_timestamp);
        }

        void add(Summary summary, TimestampType bucket_end_timestamp)
        {
            if (summary.samples.empty())
                return;
            sliding_sum.add(std::move(summary), bucket_end_timestamp);
        }

        void removeBefore(TimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/) const
        {
            const Summary combined = sliding_sum.getCurrentSum();

            /// Prometheus can't smooth with fewer than two points, and returns no value in that case.
            if (combined.samples.size() < 2)
                return std::nullopt;

            std::vector<std::pair<TimestampType, ValueType>> sorted(combined.samples.begin(), combined.samples.end()); // STYLE_CHECK_ALLOW_STD_CONTAINERS
            std::sort(sorted.begin(), sorted.end(),
                [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

            const size_t l = sorted.size();
            const Float64 sf = smoothing_factor;
            const Float64 tf = trend_factor;

            /// Initial level and trend, matching Prometheus's funcDoubleExponentialSmoothing.
            Float64 s0 = 0;
            Float64 s1 = static_cast<Float64>(sorted[0].second);
            Float64 b = static_cast<Float64>(sorted[1].second) - static_cast<Float64>(sorted[0].second);

            for (size_t i = 1; i < l; ++i)
            {
                const Float64 x = sf * static_cast<Float64>(sorted[i].second);

                /// calcTrendValue(i - 1): the trend is left unchanged on the first step (i == 1), then updated as
                /// tf * (s1 - s0) + (1 - tf) * b on subsequent steps.
                if (i != 1)
                    b = tf * (s1 - s0) + (1.0 - tf) * b;

                const Float64 y = (1.0 - sf) * (s1 + b);
                s0 = s1;
                s1 = x + y;
            }

            return static_cast<ValueType>(s1);
        }
    };

    /// The bucket stores raw samples; the aggregator's `add(const Samples &)` collects their timestamps and values.
    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 1;
};


/// Aggregate function that computes Prometheus `double_exponential_smoothing` (Holt-Winters double exponential
/// smoothing) of time series values over a sliding window on a regular time grid. It takes two extra scalar
/// parameters: the smoothing factor and the trend factor, both in the open interval (0, 1).
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid<TimestampType_, IntervalType_, ValueType_>,
        AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits<TimestampType_, IntervalType_, ValueType_>;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid, Traits>;

    explicit AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_, UInt32 timestamp_scale_,
        Float64 smoothing_factor_, Float64 trend_factor_)
        : Base(argument_types_, parameters_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , smoothing_factor(smoothing_factor_)
        , trend_factor(trend_factor_)
    {
    }

    Aggregator createAggregator(size_t /* num_populated_buckets */) const
    {
        return Aggregator{smoothing_factor, trend_factor};
    }

    static constexpr bool DateTime64Supported = true;

protected:
    const Float64 smoothing_factor{};   /// smoothing factor (sf), in (0, 1)
    const Float64 trend_factor{};       /// trend factor (tf), in (0, 1)
};

}
