#pragma once

#include <cstddef>
#include <optional>
#include <type_traits>

#include <base/defines.h>

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

/// Traits of timeSeriesLastToGrid (PromQL last_over_time) and timeSeriesTimestampOfLastToGrid (PromQL ts_of_last_over_time).
///
/// The bucket is its latest sample: the duplicate-timestamp rule keeps the greatest value at a timestamp, which
/// doesn't change which timestamp is the latest one.
template <typename TimestampType_, typename ValueType_, bool return_timestamp_>
struct AggregateFunctionTimeseriesLastToGridTraits
{
    /// Return the timestamp of the latest sample (ts_of_last_over_time) instead of its value.
    static constexpr bool return_timestamp = return_timestamp_;
    using GridScaleTimestampType = DateTime64;
    using ValueType = ValueType_;
    using TimestampType = TimestampType_;

    /// The timestamp is returned as is, with the type of the timestamps in the input columns.
    using ResultType = std::conditional_t<return_timestamp, TimestampType, ValueType>;

    static String getName()
    {
        return return_timestamp ? "timeSeriesTimestampOfLastToGrid" : "timeSeriesLastToGrid";
    }

    /// The latest sample. Samples at the same timestamp are collapsed by `timeseriesMaxValueForDuplicateTimestamp`.
    struct Summary
    {
        TimestampType timestamp{};
        ValueType value{};
        bool has_value = false;

        bool empty() const { return !has_value; }

        void add(TimestampType sample_timestamp, ValueType sample_value)
        {
            if (!has_value || sample_timestamp > timestamp)
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

    /// Sliding aggregator: the result at a grid point is the latest sample inside its window. Buckets are added in
    /// time order, so each new bucket's sample is newer than the kept one; keeping a single latest sample is enough.
    /// Once that latest sample falls out of the window every older sample is out too, so the window is then empty.
    struct Aggregator
    {
        Summary latest;
        Int64 column_to_grid_multiplier;

        explicit Aggregator(Int64 column_to_grid_multiplier_)
            : column_to_grid_multiplier(column_to_grid_multiplier_)
        {
        }

        void add(const Summary & bucket, GridScaleTimestampType /*bucket_end_timestamp*/)
        {
            /// `merge` keeps the newer sample and ignores an empty bucket.
            latest.merge(bucket);
        }

        void removeBefore(GridScaleTimestampType cut_off)
        {
            if (latest.has_value && static_cast<Int64>(latest.timestamp) * column_to_grid_multiplier <= cut_off)
                latest = Summary{};
        }

        std::optional<ResultType> getResult(GridScaleTimestampType /*grid_timestamp*/) const
        {
            if (latest.empty())
                return std::nullopt;
            if constexpr (return_timestamp)
                return latest.timestamp;
            else
                return latest.value;
        }
    };

    /// The summary itself, see above.
    using Bucket = Summary;

    static constexpr UInt16 FORMAT_VERSION = 5;
};


/// Aggregate function to calculate PromQL-like last_over_time (or ts_of_last_over_time) on a grid.
/// Missing values are filled with NULLs.
template <typename TimestampType_, typename ValueType_, bool return_timestamp_>
class AggregateFunctionTimeseriesLastToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesLastToGrid<TimestampType_, ValueType_, return_timestamp_>,
        AggregateFunctionTimeseriesLastToGridTraits<TimestampType_, ValueType_, return_timestamp_>>
{
public:
    using Traits = AggregateFunctionTimeseriesLastToGridTraits<TimestampType_, ValueType_, return_timestamp_>;

    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesLastToGrid, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return Aggregator{Base::column_to_grid_multiplier};
    }
};

}
