#pragma once

#include <cstddef>
#include <optional>
#include <type_traits>

#include <base/defines.h>

#include <Common/DequeWithMemoryTracking.h>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/timeseriesMaxValueForDuplicateTimestamp.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/// Traits of timeSeriesFirstToGrid (PromQL first_over_time) and timeSeriesTimestampOfFirstToGrid (PromQL ts_of_first_over_time).
///
/// The bucket is its earliest sample: the duplicate-timestamp rule keeps the greatest value at a timestamp, which
/// doesn't change which timestamp is the earliest one.
template <typename TimestampType_, typename ValueType_, bool return_timestamp_>
struct AggregateFunctionTimeseriesFirstToGridTraits
{
    /// Return the timestamp of the earliest sample (ts_of_first_over_time) instead of its value.
    static constexpr bool return_timestamp = return_timestamp_;
    using GridScaleTimestampType = DateTime64;
    using ValueType = ValueType_;
    using TimestampType = TimestampType_;

    /// The timestamp is returned as is, with the type of the timestamps in the input columns.
    using ResultType = std::conditional_t<return_timestamp, TimestampType, ValueType>;

    static String getName()
    {
        return return_timestamp ? "timeSeriesTimestampOfFirstToGrid" : "timeSeriesFirstToGrid";
    }

    /// The earliest sample. Samples at the same timestamp are collapsed by `timeseriesMaxValueForDuplicateTimestamp`.
    struct Summary
    {
        TimestampType timestamp{};
        ValueType value{};
        bool has_value = false;

        bool empty() const { return !has_value; }

        void add(TimestampType sample_timestamp, ValueType sample_value)
        {
            if (!has_value || sample_timestamp < timestamp)
            {
                timestamp = sample_timestamp;
                value = sample_value;
                has_value = true;
            }
            else if (sample_timestamp == timestamp)
            {
                value = timeseriesMaxValueForDuplicateTimestamp(value, sample_value);
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
    };

    /// Sliding aggregator: the result at a grid point is the earliest sample inside its window. Buckets are added in
    /// time order, so the in-window buckets form a queue: the oldest bucket holds the result until the window passes
    /// it, then the next bucket takes over. A sliding sum doesn't fit here because a bucket added at the back never
    /// changes the result, so the queue just keeps the earliest sample of every in-window bucket.
    struct Aggregator
    {
        /// A bucket of the queue: its end timestamp (in the grid scale, to compare with the window's cutoff) and its earliest sample.
        struct Entry
        {
            GridScaleTimestampType bucket_end_timestamp;
            Summary earliest;
        };

        DequeWithMemoryTracking<Entry> window;

        void add(const Summary & bucket, GridScaleTimestampType bucket_end_timestamp)
        {
            if (!bucket.empty())
                window.push_back(Entry{bucket_end_timestamp, bucket});
        }

        void removeBefore(GridScaleTimestampType cut_off)
        {
            while (!window.empty() && window.front().bucket_end_timestamp <= cut_off)
                window.pop_front();
        }

        std::optional<ResultType> getResult(GridScaleTimestampType /*grid_timestamp*/) const
        {
            if (window.empty())
                return std::nullopt;
            const Summary & earliest = window.front().earliest;
            if constexpr (return_timestamp)
                return earliest.timestamp;
            else
                return earliest.value;
        }
    };

    /// The summary itself, see above.
    using Bucket = Summary;

    static constexpr UInt16 FORMAT_VERSION = 1;
};


/// Aggregate function to calculate PromQL-like first_over_time (or ts_of_first_over_time) on a grid.
/// Missing values are filled with NULLs.
template <typename TimestampType_, typename ValueType_, bool return_timestamp_>
class AggregateFunctionTimeseriesFirstToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesFirstToGrid<TimestampType_, ValueType_, return_timestamp_>,
        AggregateFunctionTimeseriesFirstToGridTraits<TimestampType_, ValueType_, return_timestamp_>>
{
public:
    using Traits = AggregateFunctionTimeseriesFirstToGridTraits<TimestampType_, ValueType_, return_timestamp_>;

    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesFirstToGrid, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return {};
    }
};

}
