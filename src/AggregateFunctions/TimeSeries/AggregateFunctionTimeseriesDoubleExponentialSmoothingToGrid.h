#pragma once

#include <cstddef>
#include <optional>
#include <utility>

#include <Common/VectorWithMemoryTracking.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

template <typename TimestampType_, typename ValueType_>
struct AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits
{
    using GridScaleTimestampType = DateTime64;
    using TimestampType = TimestampType_;
    using ValueType = ValueType_;
    using ResultType = Float64;

    static String getName()
    {
        return "timeSeriesDoubleExponentialSmoothingToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// The bucket's sample values, or the window's values once merged. They stay in timestamp order because
    /// each bucket yields sorted samples and the window merges buckets in time order.
    struct Summary
    {
        VectorWithMemoryTracking<ValueType> samples;

        void merge(const Summary & other)
        {
            samples.insert(samples.end(), other.samples.begin(), other.samples.end());
        }
    };

    /// Sliding aggregator: keeps the window's samples in a `SlidingSum` and computes Prometheus's
    /// `double_exponential_smoothing` (Holt-Winters double exponential smoothing) per grid point.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<Summary> sliding_sum;
        Float64 smoothing_factor = 0;
        Float64 trend_factor = 0;

        Aggregator(Float64 smoothing_factor_, Float64 trend_factor_)
            : smoothing_factor(smoothing_factor_), trend_factor(trend_factor_)
        {
        }

        void add(const Samples & samples, GridScaleTimestampType bucket_end_timestamp)
        {
            Summary summary;
            samples.forEachSample([&summary](TimestampType, ValueType value)
            {
                summary.samples.push_back(value);
            });
            add(std::move(summary), bucket_end_timestamp);
        }

        void add(Summary summary, GridScaleTimestampType bucket_end_timestamp)
        {
            if (summary.samples.empty())
                return;
            sliding_sum.add(std::move(summary), bucket_end_timestamp);
        }

        void removeBefore(GridScaleTimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<ResultType> getResult(GridScaleTimestampType /*grid_timestamp*/) const
        {
            const auto & values = sliding_sum.getCurrentSum().samples;

            /// Prometheus can't smooth with fewer than two points, and returns no value in that case.
            if (values.size() < 2)
                return std::nullopt;

            const size_t l = values.size();
            const Float64 sf = smoothing_factor;
            const Float64 tf = trend_factor;

            /// Initial level and trend, matching Prometheus's funcDoubleExponentialSmoothing.
            Float64 s0 = 0;
            Float64 s1 = static_cast<Float64>(values[0]);
            Float64 b = static_cast<Float64>(values[1]) - static_cast<Float64>(values[0]);

            for (size_t i = 1; i < l; ++i)
            {
                const Float64 x = sf * static_cast<Float64>(values[i]);

                /// calcTrendValue(i - 1): the trend is left unchanged on the first step (i == 1), then updated as
                /// tf * (s1 - s0) + (1 - tf) * b on subsequent steps.
                if (i != 1)
                    b = tf * (s1 - s0) + (1.0 - tf) * b;

                const Float64 y = (1.0 - sf) * (s1 + b);
                s0 = s1;
                s1 = x + y;
            }

            return s1;
        }
    };

    /// The bucket stores raw samples; the aggregator's `add(const Samples &)` collects their timestamps and values.
    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 1;
};


/// Aggregate function that computes Prometheus `double_exponential_smoothing` (Holt-Winters double exponential
/// smoothing) of time series values over a sliding window on a regular time grid. It takes two extra scalar
/// parameters: the smoothing factor and the trend factor, both in the open interval (0, 1).
template <typename TimestampType_, typename ValueType_>
class AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid<TimestampType_, ValueType_>,
        AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits<TimestampType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits<TimestampType_, ValueType_>;

    using TimestampType = typename Traits::TimestampType;
    using ValueType = typename Traits::ValueType;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid, Traits>;
    using GridScaleTimestampType = typename Base::GridScaleTimestampType;
    using GridScaleIntervalType = typename Base::GridScaleIntervalType;

    explicit AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid(const DataTypes & argument_types_, const Array & parameters_,
        GridScaleTimestampType grid_start_, GridScaleTimestampType grid_end_, GridScaleIntervalType grid_step_, GridScaleIntervalType window_, UInt32 grid_scale_,
        UInt32 column_timestamp_scale_,
        Float64 smoothing_factor_, Float64 trend_factor_)
        : Base(argument_types_, parameters_, grid_start_, grid_end_, grid_step_, window_, grid_scale_, column_timestamp_scale_)
        , smoothing_factor(smoothing_factor_)
        , trend_factor(trend_factor_)
    {
    }

    Aggregator createAggregator(size_t /* num_populated_buckets */) const
    {
        return Aggregator{smoothing_factor, trend_factor};
    }

protected:
    const Float64 smoothing_factor{};   /// smoothing factor (sf), in (0, 1)
    const Float64 trend_factor{};       /// trend factor (tf), in (0, 1)
};

}
