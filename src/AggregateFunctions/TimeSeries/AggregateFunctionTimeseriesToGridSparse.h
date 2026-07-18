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

template <typename TimestampType_, typename IntervalType_, typename ValueType_>
struct AggregateFunctionTimeseriesToGridSparseTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return "timeSeriesResampleToGridWithStaleness";
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

    /// Sliding aggregator: the result at a grid point is the most recent sample inside its window. Buckets are
    /// added in time order, so each new bucket's sample is newer than the kept one; keeping a single newest
    /// sample is enough. Once that newest sample falls out of the window every older sample is out too, so the
    /// window is then empty.
    struct Aggregator
    {
        Summary latest;

        void add(const Summary & summary, TimestampType /*bucket_end_timestamp*/)
        {
            /// Buckets arrive in ascending time order, so a populated bucket's sample is newer than the kept one;
            /// `merge` keeps the newer sample and ignores an empty bucket.
            latest.merge(summary);
        }

        void removeBefore(TimestampType cut_off)
        {
            if (latest.has_value && latest.first <= cut_off)
                latest = Summary{};
        }

        std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/) const
        {
            if (!latest.has_value)
                return std::nullopt;
            return latest.second;
        }
    };

    /// Resample keeps no preaggregated summary - the bucket (its newest sample) is fed to the aggregator as-is.
    using Bucket = Summary;
};


/// Aggregate function to convert timeseries to the specified grid with staleness
/// Missing values are filled with NULLs
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesToGridSparse final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesToGridSparse<TimestampType_, IntervalType_, ValueType_>,
        AggregateFunctionTimeseriesToGridSparseTraits<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesToGridSparseTraits<TimestampType_, IntervalType_, ValueType_>;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesToGridSparse, Traits>;
    using Base::Base;

    typename Traits::Aggregator createAggregator(size_t /* num_populated_buckets */) const
    {
        return {};
    }

    static constexpr UInt16 FORMAT_VERSION = 3;
    static constexpr bool DateTime64Supported = true;
};

}
