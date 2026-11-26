#pragma once

#include <optional>
#include <utility>

#include <absl/container/inlined_vector.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Traits of timeSeriesDoubleExponentialSmoothingToGrid (PromQL `double_exponential_smoothing`): the values of the window
/// are smoothed in time order, each step updating a level and a trend, and the result is the last level.
template <typename TimestampType_, typename ValueType_>
struct AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits
{
    using GridScaleTimestampType = DateTime64;
    using TimestampType = TimestampType_;
    using ValueType = ValueType_;

    /// The level and the trend are fractions even for integer-like values, so the result is calculated with double precision.
    using ResultType = Float64;

    static String getName()
    {
        return "timeSeriesDoubleExponentialSmoothingToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// The bucket stores raw samples: each smoothing step depends on the previous one.
    using Bucket = Samples;

    /// Keeps the values of the current window in time order. The smoothing of one window cannot be reused for the next
    /// one, because it starts from the first value of the window, so each grid point walks its whole window.
    struct Aggregator
    {
        explicit Aggregator(Float64 smoothing_factor_, Float64 trend_factor_)
            : smoothing_factor(smoothing_factor_), trend_factor(trend_factor_)
        {
        }

        /// Adds the bucket that entered the window. Buckets are added in time order.
        void add(const Samples & samples, GridScaleTimestampType bucket_end_timestamp)
        {
            /// `getResult` relies on every stored bucket having a value.
            if (samples.maxSampleCount() == 0)
                return;

            BucketValues bucket{.bucket_end = bucket_end_timestamp, .values = {}};
            bucket.values.reserve(samples.maxSampleCount());
            samples.forEachSample([&bucket](TimestampType /*timestamp*/, ValueType value)
            {
                bucket.values.push_back(value);
            });

            buckets_in_window.push_back(std::move(bucket));
        }

        /// Drops the buckets that left the window. The window is open on the left, so a bucket that ends at `cut_off` is out.
        void removeBefore(GridScaleTimestampType cut_off)
        {
            while (!buckets_in_window.empty() && buckets_in_window.front().bucket_end <= cut_off)
                buckets_in_window.pop_front();
        }

        /// Returns the last smoothed value of the window, or nothing if the window has fewer than two values.
        [[nodiscard]] std::optional<ResultType> getResult(GridScaleTimestampType /*grid_timestamp*/) const
        {
            if (buckets_in_window.empty())
                return std::nullopt;

            /// The factors are not checked in the constructor: that would reject invalid factors even for an empty
            /// window, which Prometheus does not do.
            checkFactors();

            size_t count = 0;
            Float64 level = 0;
            Float64 previous_level = 0;
            Float64 trend = 0;

            for (const auto & bucket : buckets_in_window)
            {
                for (const ValueType bucket_value : bucket.values)
                {
                    const Float64 value = static_cast<Float64>(bucket_value);
                    if (count == 0)
                        level = value;
                    else
                    {
                        /// The first trend is the difference of the first two values. Later ones mix the last change of
                        /// the level with the previous trend.
                        if (count == 1)
                            trend = value - level;
                        else
                            trend = trend_factor * (level - previous_level) + (1 - trend_factor) * trend;

                        previous_level = level;
                        level = smoothing_factor * value + (1 - smoothing_factor) * (previous_level + trend);
                    }
                    ++count;
                }
            }

            return (count < 2) ? std::optional<ResultType>{} : level;
        }

    private:
        /// Throws if a factor is not between 0 and 1. Like in Prometheus, a NaN factor passes.
        void checkFactors() const
        {
            if (smoothing_factor <= 0 || smoothing_factor >= 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Invalid smoothing factor {} of function {}: it must be greater than 0 and less than 1", smoothing_factor, getName());
            if (trend_factor <= 0 || trend_factor >= 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Invalid trend factor {} of function {}: it must be greater than 0 and less than 1", trend_factor, getName());
        }

        /// Two values fit into the space the vector needs anyway for its heap pointer and capacity, so they cost nothing.
        static constexpr size_t INLINE_VALUES_PER_BUCKET = 2;

        /// The values of one bucket, in time order. A bucket enters and leaves the window as a whole.
        struct BucketValues
        {
            GridScaleTimestampType bucket_end;
            absl::InlinedVector<ValueType, INLINE_VALUES_PER_BUCKET, AllocatorWithMemoryTracking<ValueType>> values;
        };

        /// How much a new value moves the level: near 0 the level follows the old values, near 1 the new one.
        const Float64 smoothing_factor;
        /// How much the last change of the level moves the trend: near 0 the trend follows the old trend, near 1 the last change.
        const Float64 trend_factor;

        /// The buckets of the current window, oldest first. The bucket end is kept once per bucket, not once per value.
        DequeWithMemoryTracking<BucketValues> buckets_in_window;
    };

    static constexpr UInt16 FORMAT_VERSION = 1;
};

/// Aggregate function that calculates the PromQL `double_exponential_smoothing` of time series values on a regular time grid.
/// The two factors are parameters, so they are constants of the query and are not stored in the state.
template <typename TimestampType_, typename ValueType_>
class AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid<TimestampType_, ValueType_>,
        AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits<TimestampType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits<TimestampType_, ValueType_>;

    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid, Traits>;

    using GridScaleTimestampType = typename Base::GridScaleTimestampType;
    using GridScaleIntervalType = typename Base::GridScaleIntervalType;

    explicit AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid(const DataTypes & argument_types_, const Array & parameters_,
        GridScaleTimestampType grid_start_, GridScaleTimestampType grid_end_, GridScaleIntervalType grid_step_, GridScaleIntervalType window_,
        UInt32 grid_scale_, UInt32 column_timestamp_scale_, Float64 smoothing_factor_, Float64 trend_factor_)
        : Base(argument_types_, parameters_, grid_start_, grid_end_, grid_step_, window_, grid_scale_, column_timestamp_scale_)
        , smoothing_factor(smoothing_factor_)
        , trend_factor(trend_factor_)
    {
    }

    Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return Aggregator{smoothing_factor, trend_factor};
    }

private:
    /// How much a new value moves the level: near 0 the level follows the old values, near 1 the new one.
    const Float64 smoothing_factor;
    /// How much the last change of the level moves the trend: near 0 the trend follows the old trend, near 1 the last change.
    const Float64 trend_factor;
};

}
