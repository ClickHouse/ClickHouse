#pragma once

#include <cstddef>
#include <cstring>
#include <optional>


#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/// Traits for an aggregate function which, like `AggregateFunctionTimeseriesToGridSparseTraits`, keeps the most
/// recent sample within each grid point's window - but `getResult()` returns that sample's own TIMESTAMP
/// (converted to the "seconds since epoch" units PromQL's timestamp() function expects) instead of its value.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
struct AggregateFunctionTimeseriesTimestampToGridTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;
    /// The result is always a `DateTime64`-scaled timestamp expressed as seconds since epoch, regardless of the
    /// input sample's `ValueType` - a `Float32` value column must not constrain the timestamp's precision.
    using ResultType = Float64;

    static String getName()
    {
        return "timeSeriesTimestampToGrid";
    }

    struct Summary
    {
        TimestampType first = 0;
        ValueType second = 0;
        bool has_value = false;

        void add(TimestampType timestamp, ValueType value)
        {
            if (!has_value || timestamp > first || (timestamp == first && value > second))
            {
                first = timestamp;
                second = value;
                has_value = true;
            }
        }

        void merge(const Summary & other)
        {
            if (other.has_value)
                add(other.first, other.second);
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

    /// Sliding aggregator: keeps the single most recent sample in the window (see
    /// `AggregateFunctionTimeseriesToGridSparseTraits::Aggregator` for why that's enough), but `getResult()`
    /// returns that sample's own timestamp, scaled to seconds, rather than its value.
    struct Aggregator
    {
        Summary latest;
        TimestampType timestamp_scale_multiplier;

        explicit Aggregator(TimestampType timestamp_scale_multiplier_)
            : timestamp_scale_multiplier(timestamp_scale_multiplier_)
        {
        }

        void add(const Summary & summary, TimestampType /*bucket_end_timestamp*/)
        {
            latest.merge(summary);
        }

        void removeBefore(TimestampType cut_off)
        {
            if (latest.has_value && latest.first <= cut_off)
                latest = Summary{};
        }

        std::optional<ResultType> getResult(TimestampType /*grid_timestamp*/) const
        {
            if (!latest.has_value)
                return std::nullopt;

            /// Convert the internal timestamp (ticks of `timestamp_scale_multiplier` per second, e.g. milliseconds
            /// for DateTime64(3)) to seconds since epoch, as PromQL's timestamp() function expects.
            return static_cast<ResultType>(latest.first) / static_cast<ResultType>(timestamp_scale_multiplier);
        }
    };

    /// Resample keeps no preaggregated summary - the bucket (its newest sample) is fed to the aggregator as-is.
    using Bucket = Summary;
};


/// Aggregate function used to implement PromQL's timestamp() function: for each grid point it returns the
/// timestamp (in seconds since epoch) of the most recent sample within the window, or NULL if there is none.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesTimestampToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesTimestampToGrid<TimestampType_, IntervalType_, ValueType_>,
        AggregateFunctionTimeseriesTimestampToGridTraits<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesTimestampToGridTraits<TimestampType_, IntervalType_, ValueType_>;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesTimestampToGrid, Traits>;
    using Base::Base;

    typename Traits::Aggregator createAggregator(size_t /* num_populated_buckets */) const
    {
        return typename Traits::Aggregator{Base::timestamp_scale_multiplier};
    }

    static constexpr UInt16 FORMAT_VERSION = 3;
    static constexpr bool DateTime64Supported = true;
};

}
