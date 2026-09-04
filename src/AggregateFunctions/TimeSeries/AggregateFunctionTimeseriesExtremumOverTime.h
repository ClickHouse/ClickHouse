#pragma once

#include <cstddef>
#include <optional>


#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>
#include <AggregateFunctions/TimeSeries/timeseriesMaxValueForDuplicateTimestamp.h>
#include <Common/NaNUtils.h>


namespace DB
{

template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_max_>
struct AggregateFunctionTimeseriesExtremumOverTimeTraits
{
    static constexpr bool is_max = is_max_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return is_max ? "timeSeriesMaxToGrid" : "timeSeriesMinToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Running extremum (max or min): the value and the timestamp of whichever sample folded into it so far
    /// currently holds it.
    struct Summary
    {
        TimestampType first = 0;   /// timestamp of the sample that is currently the running extremum
        ValueType second = 0;      /// value of the running extremum
        bool has_value = false;

        /// Order over samples: at the same timestamp the `timeSeries*` duplicate rule applies - the greatest
        /// value survives, a NaN losing to a real one, values `==` cannot separate decided by raw bits
        /// (`timeseriesMaxValueForDuplicateTimestamp`) - and across timestamps a real beats NaN, among NaNs
        /// the latest timestamp wins, among IEEE-equal reals the earliest wins - matching a time-ordered
        /// PromQL scan and keeping add/merge commutative. A `Summary` is fed from deduplicated buckets with
        /// disjoint timestamp ranges, so it only ever compares samples at distinct timestamps, where the
        /// order is total.
        bool shouldReplace(TimestampType timestamp, ValueType value) const
        {
            if (!has_value)
                return true;
            if (timestamp == first)
            {
                if (isNaN(second))
                {
                    if (!isNaN(value))
                        return true;
                    return timeseriesHasGreaterBits(value, second);
                }
                if (isNaN(value))
                    return false;
                if (value > second)
                    return true;
                if (value < second)
                    return false;
                return timeseriesHasGreaterBits(value, second);
            }
            if (isNaN(second))
            {
                if (!isNaN(value))
                    return true;
                return timestamp > first;
            }
            if (isNaN(value))
                return false;
            if constexpr (is_max)
            {
                if (value > second)
                    return true;
            }
            else
            {
                if (value < second)
                    return true;
            }
            if (value != second)
                return false;
            return timestamp < first;
        }

        void add(TimestampType timestamp, ValueType value)
        {
            if (shouldReplace(timestamp, value))
            {
                first = timestamp;
                second = value;
                has_value = true;
            }
        }

        /// Commutative and associative over what the aggregator feeds it: the combined buckets cover disjoint
        /// timestamp ranges, so `shouldReplace` only ever compares distinct timestamps, where it is a total
        /// order. Two-Stacks' out-of-order combining therefore returns bit-for-bit what a time-ordered fold
        /// returns, including the surviving NaN payload.
        void merge(const Summary & other)
        {
            if (other.has_value && shouldReplace(other.first, other.second))
            {
                first = other.first;
                second = other.second;
                has_value = true;
            }
        }
    };

    /// Sliding aggregator: preaggregates each bucket's samples into a running extremum, then keeps the running
    /// combine (max or min) of the per-bucket summaries within the window. Unlike last_over_time, the window's
    /// extremum is not necessarily held by the most recently added bucket, so dropping a stale bucket can
    /// require recomputing the extremum from the remaining ones - `Summary` is not invertible (it has no
    /// `unmerge`), but its `merge` is commutative and associative, so
    /// `AggregateFunctionTimeseriesSlidingSum` handles this correctly via its recompute (or two-stacks) strategy.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;

        explicit Aggregator(size_t stack_size) : sliding_sum(stack_size) {}

        /// Preaggregate the bucket's samples (`forEachSample` visits them in ascending timestamp order with
        /// duplicates already collapsed to the greatest value at each timestamp) into a per-bucket extremum.
        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            Summary summary;
            samples.forEachSample([&summary](TimestampType timestamp, ValueType value)
            {
                summary.add(timestamp, value);
            });
            add(std::move(summary), bucket_end_timestamp);
        }

        void add(Summary summary, TimestampType bucket_end_timestamp)
        {
            if (summary.has_value)
                sliding_sum.add(std::move(summary), bucket_end_timestamp);
        }

        void removeBefore(TimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/) const
        {
            const Summary combined = sliding_sum.getCurrentSum();
            if (!combined.has_value)
                return std::nullopt;
            return combined.second;
        }
    };

    /// The bucket stores raw samples like the rest of the `timeSeries*` family: `AggregateFunctionTimeseriesSamples`
    /// applies the duplicate-timestamp rule (collapse to the greatest value at a timestamp) before the extremum,
    /// and buckets cover disjoint timestamp ranges, so the window's `Summary` sees each timestamp exactly once
    /// with its canonical value.
    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 2;

    /// `getStackSizeForTwoStacks` switches to the two-stack queue once the average number of populated buckets in a
    /// window reaches this value; below it, recomputing the window each grid point is cheaper. The
    /// `timeseries_to_grid_two_stack_vs_recompute` example measures this summary directly: its two-stacks
    /// crossover sits at ~2 populated buckets per window and its 2x point at ~14 (Apple M-class; the
    /// linear-regression summary lands at 4 and 12 on the same machine), both below the 10/20 the
    /// linear-regression functions use. Keeping the shared 10/20 is therefore conservative for this summary -
    /// two-stacks is only enabled where it measures well ahead - and keeps one selector policy across the
    /// non-invertible functions.
    static constexpr size_t AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS = 10;

    /// Hard cap: regardless of average density, use two-stacks once a window can hold this many buckets (see
    /// the sibling constant in `AggregateFunctionTimeseriesLinearRegression` for why an average alone is not
    /// enough).
    static constexpr size_t BPW_TO_FORCE_TWO_STACKS = 20;
};


/// Aggregate function to calculate PromQL-like max_over_time/min_over_time on a grid.
template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_max_>
class AggregateFunctionTimeseriesExtremumOverTime final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesExtremumOverTime<TimestampType_, IntervalType_, ValueType_, is_max_>,
        AggregateFunctionTimeseriesExtremumOverTimeTraits<TimestampType_, IntervalType_, ValueType_, is_max_>>
{
public:
    using Traits = AggregateFunctionTimeseriesExtremumOverTimeTraits<TimestampType_, IntervalType_, ValueType_, is_max_>;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesExtremumOverTime, Traits>;
    using Base::Base;

    typename Traits::Aggregator createAggregator(size_t stack_size_for_two_stacks) const
    {
        return typename Traits::Aggregator{stack_size_for_two_stacks};
    }
};

/// Each SQL function as a 3-argument template with its is_max variant baked in, so registration names the
/// function directly.
template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesMaxToGrid = AggregateFunctionTimeseriesExtremumOverTime<TimestampType, IntervalType, ValueType, true>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesMinToGrid = AggregateFunctionTimeseriesExtremumOverTime<TimestampType, IntervalType, ValueType, false>;

}
