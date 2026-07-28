#pragma once

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
            if (!has_value || isBetter(value, second))
            {
                first = timestamp;
                second = value;
                has_value = true;
            }
        }

        /// Commutative and associative: keeps whichever of the two summaries holds the "better" extremum. This
        /// is what makes max/min combinable in any order, unlike last_over_time's "most recent" which requires
        /// buckets to be combined in time order.
        void merge(const Summary & other)
        {
            if (!other.has_value)
                return;
            if (!has_value || isBetter(other.second, second))
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

    typename Traits::Aggregator createAggregator(size_t /* num_populated_buckets */) const
    {
        return {};
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
