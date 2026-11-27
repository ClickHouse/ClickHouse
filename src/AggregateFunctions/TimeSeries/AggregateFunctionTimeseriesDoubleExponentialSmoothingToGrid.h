#pragma once

#include <optional>
#include <utility>

#include <absl/container/inlined_vector.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesScalarOrGridArgument.h>
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
        [[nodiscard]] std::optional<ResultType> getResult(GridScaleTimestampType /*grid_timestamp*/, Float64 smoothing_factor, Float64 trend_factor) const
        {
            if (buckets_in_window.empty())
                return std::nullopt;

            /// The factors are checked only for a window with samples: like Prometheus, an empty window gives no result
            /// even with invalid factors.
            checkFactors(smoothing_factor, trend_factor);

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
        static void checkFactors(Float64 smoothing_factor, Float64 trend_factor)
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

        /// The buckets of the current window, oldest first. The bucket end is kept once per bucket, not once per value.
        DequeWithMemoryTracking<BucketValues> buckets_in_window;
    };

    static constexpr UInt16 FORMAT_VERSION = 1;
};

/// Aggregate function that calculates the PromQL `double_exponential_smoothing` of time series values on a regular time grid.
/// The two factors are the two arguments after the samples, each a number or an array with one value per grid point.
template <typename TimestampType_, typename ValueType_>
class AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid<TimestampType_, ValueType_>,
        AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits<TimestampType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits<TimestampType_, ValueType_>;

    using ResultType = typename Traits::ResultType;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid, Traits>;
    using Base::Base;

    /// The smoothing factor and the trend factor are two more arguments after the samples, kept in the state.
    static constexpr size_t num_extra_arguments = 2;

    struct State : Base::State
    {
        /// How much a new value moves the level: near 0 the level follows the old values, near 1 the new one.
        AggregateFunctionTimeseriesScalarOrGridArgument smoothing_factor;

        /// How much the last change of the level moves the trend: near 0 the trend follows the old trend, near 1 the last change.
        AggregateFunctionTimeseriesScalarOrGridArgument trend_factor;
    };

    /// The names used in the error messages for the smoothing factor and the trend factor.
    static constexpr TimeseriesScalarOrGridArgumentNames smoothing_factor_names{"timeSeriesDoubleExponentialSmoothingToGrid", "smoothing_factor"};
    static constexpr TimeseriesScalarOrGridArgumentNames trend_factor_names{"timeSeriesDoubleExponentialSmoothingToGrid", "trend_factor"};

    Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return Aggregator{};
    }

    void addExtraArguments(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** extra_columns,
        const UInt8 * flags, bool flag_value_to_include) const
    {
        data(place)->smoothing_factor.captureOrCheck(smoothing_factor_names, Base::grid_size, row_begin, row_end, *extra_columns[0], flags, flag_value_to_include);
        data(place)->trend_factor.captureOrCheck(trend_factor_names, Base::grid_size, row_begin, row_end, *extra_columns[1], flags, flag_value_to_include);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        Base::mergeImpl(place, rhs, arena);
        data(place)->smoothing_factor.merge(smoothing_factor_names, data(rhs)->smoothing_factor);
        data(place)->trend_factor.merge(trend_factor_names, data(rhs)->trend_factor);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        Base::serialize(place, buf, version);
        data(place)->smoothing_factor.serialize(buf);
        data(place)->trend_factor.serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        Base::deserialize(place, buf, version, arena);
        data(place)->smoothing_factor.deserialize(smoothing_factor_names, buf, Base::grid_size);
        data(place)->trend_factor.deserialize(trend_factor_names, buf, Base::grid_size);
    }

    std::optional<ResultType> getGridPointResult(const Aggregator & aggregator, ConstAggregateDataPtr place, size_t grid_index) const
    {
        return aggregator.getResult(Base::getGridPoint(grid_index), data(place)->smoothing_factor.at(grid_index), data(place)->trend_factor.at(grid_index));
    }

private:
    static const State * data(ConstAggregateDataPtr __restrict place)
    {
        return reinterpret_cast<const State *>(place);
    }

    static State * data(AggregateDataPtr __restrict place)
    {
        return reinterpret_cast<State *>(place);
    }
};

}
