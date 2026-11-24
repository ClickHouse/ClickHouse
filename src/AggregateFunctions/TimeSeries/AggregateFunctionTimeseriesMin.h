#pragma once

#include <cstddef>
#include <optional>
#include <type_traits>
#include <utility>

#include <Common/NaNUtils.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

/// Traits of timeSeriesMinToGrid (PromQL min_over_time) and timeSeriesTimestampOfMinToGrid (PromQL ts_of_min_over_time).
///
/// The bucket keeps the raw samples because the duplicate-timestamp rule keeps the greatest value at a timestamp,
/// which may raise the minimum, and a preaggregated minimum could not undo a smaller value it has already taken.
template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool return_timestamp_>
struct AggregateFunctionTimeseriesMinTraits
{
    /// Return the timestamp of the minimum (ts_of_min_over_time) instead of the minimum itself.
    static constexpr bool return_timestamp = return_timestamp_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    /// A timestamp in seconds needs Float64 precision (a Float32 near the current epoch has a ~128-second ulp).
    using ResultType = std::conditional_t<return_timestamp, Float64, ValueType>;

    static String getName()
    {
        return return_timestamp ? "timeSeriesTimestampOfMinToGrid" : "timeSeriesMinToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// The minimum and the timestamp of the sample holding it. It only sees samples at distinct timestamps (buckets
    /// are deduplicated and cover disjoint time ranges). A real value beats a NaN, and among equal values (or NaNs)
    /// the latest timestamp wins, like in Prometheus. The order is total, so `merge` is commutative and associative.
    struct Summary
    {
        TimestampType timestamp{};
        ValueType value{};
        bool has_value = false;

        bool empty() const { return !has_value; }

        void add(TimestampType sample_timestamp, ValueType sample_value)
        {
            if (shouldReplace(sample_timestamp, sample_value))
            {
                timestamp = sample_timestamp;
                value = sample_value;
                has_value = true;
            }
        }

        void merge(const Summary & other)
        {
            if (other.has_value)
                add(other.timestamp, other.value);
        }

    private:
        bool shouldReplace(TimestampType candidate_timestamp, ValueType candidate_value) const
        {
            if (!has_value)
                return true;

            if (isNaN(value))
                return !isNaN(candidate_value) || candidate_timestamp > timestamp;

            if (isNaN(candidate_value))
                return false;

            if (candidate_value != value)
                return candidate_value < value;

            return candidate_timestamp > timestamp;
        }
    };

    /// Sliding aggregator: preaggregates each bucket's samples into a `Summary` for the (non-invertible) `SlidingSum`.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;
        TimestampType timestamp_scale_multiplier;

        Aggregator(size_t stack_size, TimestampType timestamp_scale_multiplier_)
            : sliding_sum(stack_size), timestamp_scale_multiplier(timestamp_scale_multiplier_)
        {
        }

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            Summary summary;
            samples.forEachSample([&summary](TimestampType timestamp, ValueType value)
            {
                summary.add(timestamp, value);
            });
            if (!summary.empty())
                sliding_sum.add(std::move(summary), bucket_end_timestamp);
        }

        void removeBefore(TimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<ResultType> getResult(TimestampType /*grid_timestamp*/) const
        {
            const Summary combined = sliding_sum.getCurrentSum();
            if (combined.empty())
                return std::nullopt;
            if constexpr (return_timestamp)
            {
                /// The timestamp in seconds.
                return static_cast<Float64>(static_cast<Int64>(combined.timestamp))
                    / static_cast<Float64>(static_cast<Int64>(timestamp_scale_multiplier));
            }
            else
            {
                return combined.value;
            }
        }
    };

    /// Raw samples, preaggregated by the aggregator's `add`.
    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 1;

    /// Two-stacks thresholds, measured by the `timeseries_to_grid_two_stack_vs_recompute` example:
    /// two-stacks first wins at 4 buckets per window and is 2x faster from 18.
    static constexpr size_t AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS = 4;
    static constexpr size_t BPW_TO_FORCE_TWO_STACKS = 18;
};


/// Aggregate function to calculate PromQL-like min_over_time (or ts_of_min_over_time) on a grid.
template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool return_timestamp_>
class AggregateFunctionTimeseriesMin final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesMin<TimestampType_, IntervalType_, ValueType_, return_timestamp_>,
        AggregateFunctionTimeseriesMinTraits<TimestampType_, IntervalType_, ValueType_, return_timestamp_>>
{
public:
    using Traits = AggregateFunctionTimeseriesMinTraits<TimestampType_, IntervalType_, ValueType_, return_timestamp_>;

    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesMin, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t stack_size_for_two_stacks) const
    {
        return Aggregator{stack_size_for_two_stacks, Base::timestamp_scale_multiplier};
    }
};

/// Each SQL function as a 3-argument template, so registration names the function directly.
template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesMinToGrid = AggregateFunctionTimeseriesMin<TimestampType, IntervalType, ValueType, /* return_timestamp = */ false>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesTimestampOfMinToGrid = AggregateFunctionTimeseriesMin<TimestampType, IntervalType, ValueType, /* return_timestamp = */ true>;

}
