#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <optional>


#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>

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

    /// Returns whether `candidate` should replace `current` as the running extremum. Mirrors PromQL's
    /// `if v > max || isnan(max) { max = v }` (and the symmetric rule for min): a NaN currently held as the
    /// running extremum is unconditionally overwritten by the next sample (even another NaN), but once a real
    /// number is the extremum, a later NaN is silently ignored.
    static bool isBetter(ValueType candidate, ValueType current)
    {
        if constexpr (is_max)
            return candidate > current || std::isnan(current);
        else
            return candidate < current || std::isnan(current);
    }

    /// Per-bucket (and, once combined, per-window) running extremum: the value and the timestamp of whichever
    /// sample is currently the extremum.
    struct Summary
    {
        TimestampType first = 0;   /// timestamp of the sample that is currently the running extremum
        ValueType second = 0;      /// value of the running extremum
        bool has_value = false;

        void add(TimestampType timestamp, ValueType value)
        {
            /// An IEEE-equal value (e.g. -0.0 vs +0.0) keeps the earliest sample, like PromQL's
            /// time-ordered `if v > max` scan; this makes add/merge order-independent bit-for-bit.
            if (!has_value || isBetter(value, second) || (value == second && timestamp < first))
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

        /// Commutative and associative: strictly better value wins, an IEEE-equal value resolves to the
        /// earliest timestamp (see add), so the Two-Stacks strategy, which combines out of arrival
        /// order, returns bit-for-bit what a time-ordered fold returns (an all-NaN run may keep a
        /// different NaN, whose returned value is NaN either way).
        void merge(const Summary & other)
        {
            if (!other.has_value)
                return;
            if (!has_value || isBetter(other.second, second) || (other.second == second && other.first < first))
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

    /// `createAggregator` switches to the two-stack queue once the average number of populated buckets in a
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

    typename Traits::Aggregator createAggregator(size_t num_populated_buckets) const
    {
        /// Recompute folds the populated buckets in each window - on average `buckets_per_window * density`, where
        /// `density = num_populated_buckets / bucket_count`. Compare that average (not the dense maximum
        /// `buckets_per_window`) to the threshold, so sparse data, whose windows hold fewer populated buckets,
        /// stays on the cheaper recompute path without inflating the threshold. The hard cap still forces
        /// two-stacks for large windows, where a non-uniform spread could hide a locally dense window.
        const size_t avg_buckets_in_window = Base::bucket_count
            ? static_cast<size_t>(static_cast<double>(Base::buckets_per_window) * static_cast<double>(num_populated_buckets)
                / static_cast<double>(Base::bucket_count))
            : 0;
        const bool use_two_stacks = avg_buckets_in_window >= AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS
            || Base::buckets_per_window >= BPW_TO_FORCE_TWO_STACKS;
        /// Reserve at most `buckets_per_window`, but capped by `num_populated_buckets` - else a huge window
        /// (forced onto two-stacks by the hard cap) would `reserve(~INT64_MAX)` and fail to allocate.
        const size_t stack_size = use_two_stacks ? std::min(Base::buckets_per_window, num_populated_buckets) : 0;
        return typename Traits::Aggregator{stack_size};
    }

    static constexpr UInt16 FORMAT_VERSION = 1;
    static constexpr bool DateTime64Supported = true;
};

/// Each SQL function as a 3-argument template with its is_max variant baked in, so registration names the
/// function directly.
template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesMaxToGrid = AggregateFunctionTimeseriesExtremumOverTime<TimestampType, IntervalType, ValueType, true>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesMinToGrid = AggregateFunctionTimeseriesExtremumOverTime<TimestampType, IntervalType, ValueType, false>;

}
