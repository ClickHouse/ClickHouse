#pragma once

#include <bit>
#include <cmath>
#include <cstddef>
#include <optional>
#include <type_traits>


#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

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

    /// Per-bucket (and, once combined, per-window) running extremum: the value and the timestamp of whichever
    /// sample is currently the extremum.
    struct Summary
    {
        TimestampType first = 0;   /// timestamp of the sample that is currently the running extremum
        ValueType second = 0;      /// value of the running extremum
        bool has_value = false;

        /// Last tie-break for samples `==` cannot separate at the same timestamp (two NaN payloads,
        /// or -0.0 against +0.0): raw bits, so the surviving payload does not depend on merge order.
        static bool hasGreaterBits(ValueType lhs, ValueType rhs)
        {
            using Bits = std::conditional_t<sizeof(ValueType) == sizeof(UInt32), UInt32, UInt64>;
            return std::bit_cast<Bits>(lhs) > std::bit_cast<Bits>(rhs);
        }

        /// Total order over (value, timestamp, bits): a real beats NaN, among NaNs the latest timestamp wins, among
        /// IEEE-equal reals the earliest wins - matching a time-ordered PromQL scan and keeping add/merge commutative.
        bool shouldReplace(TimestampType timestamp, ValueType value) const
        {
            if (!has_value)
                return true;
            if (std::isnan(static_cast<double>(second)))
            {
                if (!std::isnan(static_cast<double>(value)))
                    return true;
                if (timestamp != first)
                    return timestamp > first;
                return hasGreaterBits(value, second);
            }
            if (std::isnan(static_cast<double>(value)))
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
            if (timestamp != first)
                return timestamp < first;
            return hasGreaterBits(value, second);
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

        void addMany(const TimestampType * timestamps, const ValueType * values, size_t batch_size)
        {
            for (size_t i = 0; i < batch_size; ++i)
                add(timestamps[i], values[i]);
        }

        /// Commutative and associative (shouldReplace is a total order), so Two-Stacks' out-of-order combining
        /// returns bit-for-bit what a time-ordered fold returns, including the surviving NaN payload.
        void merge(const Summary & other)
        {
            if (other.has_value && shouldReplace(other.first, other.second))
            {
                first = other.first;
                second = other.second;
                has_value = true;
            }
        }

        void serialize(WriteBuffer & buf) const
        {
            writeBinary(has_value, buf);
            writeBinaryLittleEndian(first, buf);
            writeBinaryLittleEndian(second, buf);
        }

        void deserialize(ReadBuffer & buf)
        {
            readBinary(has_value, buf);
            readBinaryLittleEndian(first, buf);
            readBinaryLittleEndian(second, buf);
        }

        template <typename RangeType>
        void checkTimestampsInRange(const RangeType & range) const
        {
            if (has_value && !range.contains(first))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Cannot deserialize data: timestamp {} is outside its bucket's range",
                    static_cast<Int64>(first));
        }
    };

    /// Sliding aggregator: keeps the running combine (max or min) of the per-bucket summaries within the window.
    /// Unlike last_over_time, the window's extremum is not necessarily held by the most recently added bucket, so
    /// dropping a stale bucket can require recomputing the extremum from the remaining ones - `Summary` is not
    /// invertible (it has no `unmerge`), but its `merge` is commutative and associative, so
    /// `AggregateFunctionTimeseriesSlidingSum` handles this correctly via its recompute (or two-stacks) strategy.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;

        explicit Aggregator(size_t stack_size) : sliding_sum(stack_size) {}

        void add(const Summary & summary, TimestampType bucket_end_timestamp)
        {
            if (summary.has_value)
                sliding_sum.add(Summary(summary), bucket_end_timestamp);
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

    /// The bucket stores the running extremum directly - no raw-sample preaggregation is needed since combining
    /// is commutative/associative regardless of the samples' arrival order.
    using Bucket = Summary;

    static constexpr UInt16 FORMAT_VERSION = 1;

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
