#pragma once

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


namespace DB
{

template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_rate_>
struct AggregateFunctionTimeSeriesLastToGridTraits
{
    static constexpr bool array_arguments = array_arguments_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName() { return "timeSeriesLastToGrid"; }

    /// Stores one sample with most recent timestamp.
    /// If there are two samples with the same timestamp, the one with bigger value is stored.
    struct Bucket
    {
        ValueType value;
        TimestampType timestamp;
        bool filled = false;

        void add(TimestampType timestamp_, ValueType value_)
        {
            if (!filled)
            {
                timestamp = timestamp_;
                value = value_;
                filled = true;
            }
            else
            {
                if (timestamp_ > timestamp)
                {
                    timestamp = timestamp_;
                    value = value_;
                }
                else if (timestamp_ == timestamp)
                {
                    /// Replace the value with bigger one
                    value = std::max(value, value_);
                }
            }
        }

        void merge(const Bucket & rhs)
        {
            if (rhs.filled)
                add(rhs.timestamp, rhs.value);
        }
    };
};


/// Aggregate function to find most recent values on the specified grid. It implements the logic of promql function last_over_time().
template <typename Traits>
class AggregateFunctionTimeSeriesLastToGrid final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeSeriesLastToGrid<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeSeriesLastToGrid<Traits>, Traits>;
    using Bucket = typename Base::Bucket;
    using Base::Base;

    static void serializeBucket(const Bucket & bucket, WriteBuffer & buf)
    {
        writeBinaryLittleEndian(bucket.filled, buf);
        if (bucket.filled)
        {
            writeBinaryLittleEndian(bucket.timestamp, buf);
            writeBinaryLittleEndian(bucket.value, buf);
        }
    }

    void deserializeBucket(Bucket & bucket, ReadBuffer & buf, const size_t bucket_index) const
    {
        readBinaryLittleEndian(bucket.filled, buf);

        if (bucket.filled)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            Base::checkTimestampInRange(timestamp, bucket_index);
            bucket.timestamp = timestamp;
            readBinaryLittleEndian(bucket.value, buf);
        }
    }

    /// Insert the result into the column.
    void doInsertResultInto(AggregateDataPtr __restrict place, IColumn & to) const
    {
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + Base::bucket_count);

        if (!Base::bucket_count)
            return;

        ColumnNullable & result_to = typeid_cast<ColumnNullable &>(arr_to.getData());
        auto & data_to = typeid_cast<typename Base::ColVecResultType &>(result_to.getNestedColumn()).getData();
        auto & nulls_to = result_to.getNullMapData();

        const size_t old_size = data_to.size();
        chassert(old_size == nulls_to.size(), "Sizes of nested column and null map of Nullable column are not equal");

        data_to.resize(old_size + Base::bucket_count);
        nulls_to.resize(old_size + Base::bucket_count);

        ValueType * values = data_to.data() + old_size;
        UInt8 * nulls = nulls_to.data() + old_size;

        const auto & buckets = Base::data(place)->buckets;

        /// Fill the data for missing buckets.
        TimestampType current_timestamp = Base::start_timestamp;
        Bucket last_sample; /// Sliding window with last sample

        for (size_t i = 0; i < Base::bucket_count; ++i, current_timestamp += Base::step)
        {
            values[i] = ValueType{};
            nulls[i] = 1;

            auto bucket_it = buckets.find(i);
            if (bucket_it != buckets.end())
                last_sample.merge(bucket_it->second);

            /// If the recent sample is within the window, we add it to the result column.
            if (last_sample.filled && last_sample.timestamp + Base::window >= current_timestamp)
            {
                values[i] = last_sample.value;
                nulls[i] = 0;
            }
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 1;
};

}
