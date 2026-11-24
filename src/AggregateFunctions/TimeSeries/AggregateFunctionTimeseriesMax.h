#pragma once

#include <cstddef>
#include <optional>
#include <type_traits>

#include <base/defines.h>

#include <Common/Exception.h>
#include <Common/NaNUtils.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>
#include <AggregateFunctions/TimeSeries/timeseriesMaxValueForDuplicateTimestamp.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/// Traits of timeSeriesMaxToGrid (PromQL max_over_time) and timeSeriesTimestampOfMaxToGrid (PromQL ts_of_max_over_time).
///
/// The bucket is a preaggregated maximum instead of the raw samples: the duplicate-timestamp rule keeps the greatest
/// value at a timestamp, which is what the maximum keeps anyway (unlike the minimum, see `AggregateFunctionTimeseriesMin.h`).
template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool return_timestamp_>
struct AggregateFunctionTimeseriesMaxTraits
{
    /// Return the timestamp of the maximum (ts_of_max_over_time) instead of the maximum itself.
    static constexpr bool return_timestamp = return_timestamp_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    /// A timestamp in seconds needs Float64 precision (a Float32 near the current epoch has a ~128-second ulp).
    using ResultType = std::conditional_t<return_timestamp, Float64, ValueType>;

    static String getName()
    {
        return return_timestamp ? "timeSeriesTimestampOfMaxToGrid" : "timeSeriesMaxToGrid";
    }

    /// The maximum and the timestamp of the sample holding it. Samples at the same timestamp are collapsed by
    /// `timeseriesMaxValueForDuplicateTimestamp`. Otherwise a real value beats a NaN, and among equal values (or NaNs)
    /// the latest timestamp wins, like in Prometheus. The order is total, so `merge` is commutative and associative.
    struct Summary
    {
        TimestampType timestamp{};
        ValueType value{};
        bool has_value = false;

        bool empty() const { return !has_value; }

        void add(TimestampType sample_timestamp, ValueType sample_value)
        {
            if (has_value && sample_timestamp == timestamp)
            {
                value = timeseriesMaxValueForDuplicateTimestamp(value, sample_value);
            }
            else if (shouldReplace(sample_timestamp, sample_value))
            {
                timestamp = sample_timestamp;
                value = sample_value;
                has_value = true;
            }
        }

        /// Bulk `add`, for the batch bucketing kernel of `AggregateFunctionTimeseriesBase`.
        ALWAYS_INLINE void addMany(const TimestampType * __restrict timestamps, const ValueType * __restrict values, size_t count)
        {
            for (size_t i = 0; i < count; ++i)
                add(timestamps[i], values[i]);
        }

        void merge(const Summary & other)
        {
            if (other.has_value)
                add(other.timestamp, other.value);
        }

        void serialize(WriteBuffer & buf) const
        {
            writeBinary(has_value, buf);
            writeBinaryLittleEndian(timestamp, buf);
            writeBinaryLittleEndian(value, buf);
        }

        void deserialize(ReadBuffer & buf)
        {
            readBinary(has_value, buf);
            readBinaryLittleEndian(timestamp, buf);
            readBinaryLittleEndian(value, buf);
        }

        template <typename RangeType>
        void checkTimestampsInRange(const RangeType & range) const
        {
            if (has_value && !range.contains(timestamp))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Cannot deserialize data: timestamp {} is outside its bucket's range",
                    static_cast<Int64>(timestamp));
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
                return candidate_value > value;

            return candidate_timestamp > timestamp;
        }
    };

    /// Sliding aggregator: buckets are summaries already and are fed to the (non-invertible) `SlidingSum` as is.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;
        TimestampType timestamp_scale_multiplier;

        Aggregator(size_t stack_size, TimestampType timestamp_scale_multiplier_)
            : sliding_sum(stack_size), timestamp_scale_multiplier(timestamp_scale_multiplier_)
        {
        }

        void add(const Summary & bucket, TimestampType bucket_end_timestamp)
        {
            if (!bucket.empty())
                sliding_sum.add(Summary{bucket}, bucket_end_timestamp);
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

    /// The summary itself, see above.
    using Bucket = Summary;

    static constexpr UInt16 FORMAT_VERSION = 1;

    /// Two-stacks thresholds, measured by the `timeseries_to_grid_two_stack_vs_recompute` example:
    /// two-stacks first wins at 4 buckets per window and is 2x faster from 18.
    static constexpr size_t AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS = 4;
    static constexpr size_t BPW_TO_FORCE_TWO_STACKS = 18;
};


/// Aggregate function to calculate PromQL-like max_over_time (or ts_of_max_over_time) on a grid.
template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool return_timestamp_>
class AggregateFunctionTimeseriesMax final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesMax<TimestampType_, IntervalType_, ValueType_, return_timestamp_>,
        AggregateFunctionTimeseriesMaxTraits<TimestampType_, IntervalType_, ValueType_, return_timestamp_>>
{
public:
    using Traits = AggregateFunctionTimeseriesMaxTraits<TimestampType_, IntervalType_, ValueType_, return_timestamp_>;

    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesMax, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t stack_size_for_two_stacks) const
    {
        return Aggregator{stack_size_for_two_stacks, Base::timestamp_scale_multiplier};
    }
};

/// Each SQL function as a 3-argument template, so registration names the function directly.
template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesMaxToGrid = AggregateFunctionTimeseriesMax<TimestampType, IntervalType, ValueType, /* return_timestamp = */ false>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesTimestampOfMaxToGrid = AggregateFunctionTimeseriesMax<TimestampType, IntervalType, ValueType, /* return_timestamp = */ true>;

}
