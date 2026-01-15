#pragma once

#include <cstddef>
#include <cstring>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionLast2Samples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_rate_>
struct AggregateFunctionTimeseriesInstantValueTraits
{
    static constexpr bool array_arguments = array_arguments_;
    static constexpr bool is_rate = is_rate_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return is_rate_ ? "timeSeriesInstantRateToGrid" : "timeSeriesInstantDeltaToGrid";
    }

    using Bucket = typename AggregateFunctionLast2Samples<TimestampType, ValueType>::Data;
};


/// Aggregate function to calculate instant values (irate and idelta) of timeseries on the specified grid
template <typename Traits>
class AggregateFunctionTimeseriesInstantValue final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesInstantValue<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    static constexpr bool is_rate = Traits::is_rate;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesInstantValue<Traits>, Traits>;

    using Bucket = typename Base::Bucket;

    using Base::Base;

    static void serializeBucket(const Bucket & bucket, WriteBuffer & buf)
    {
        writeBinaryLittleEndian(bucket.filled, buf);
        for (size_t i = 0; i < bucket.filled; ++i)
        {
            writeBinaryLittleEndian(bucket.timestamps[i], buf);
        }
        for (size_t i = 0; i < bucket.filled; ++i)
        {
            writeBinaryLittleEndian(bucket.values[i], buf);
        }
    }

    void deserializeBucket(Bucket & bucket, ReadBuffer & buf, const size_t bucket_index) const
    {
        readBinaryLittleEndian(bucket.filled,buf);

        if (bucket.filled > 2)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with more than 2 samples in a bucket");

        for (size_t j = 0; j < bucket.filled; ++j)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            Base::checkTimestampInRange(timestamp, bucket_index);
            bucket.timestamps[j] = timestamp;
        }
        for (size_t j = 0; j < bucket.filled; ++j)
        {
            readBinaryLittleEndian(bucket.values[j], buf);
        }
    }

    void fillResultValue(TimestampType timestamp, ValueType value, TimestampType previous_timestamp, ValueType previous_value, ValueType & result, UInt8 & null) const
    {
        ValueType time_difference = timestamp - previous_timestamp;
        if (time_difference == 0)
        {
            result = 0;
            null = 1;
            return;
        }

        /// Resets must be take into account for `irate` function because it expects counter timeseries that only increase.
        /// But `idelta` function expects gauge timeseries that can decrease and it is not considered to be a reset.
        constexpr bool adjust_to_resets = is_rate;

        ValueType value_difference = (adjust_to_resets && value < previous_value) ? value : (value - previous_value);
        result = value_difference;
        if constexpr (is_rate)
            result = result * Base::timestamp_scale_multiplier / time_difference;
        null = 0;
    }

    /// Insert the result into the column
    /// timestamps buffer is reused between calls to avoid unnecessary allocations/deallocations
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

        /// Fill the data for missing buckets
        TimestampType current_timestamp = Base::start_timestamp;
        Bucket last_2_samples; /// Sliding window with last 2 samples
        for (size_t i = 0; i < Base::bucket_count; ++i, current_timestamp += Base::step)
        {
            values[i] = ValueType{};
            nulls[i] = 1;

            auto bucket_it = buckets.find(i);
            if (bucket_it != buckets.end())
                last_2_samples.merge(bucket_it->second);

            /// If the oldest of last 2 samples is within the window, we can calculate the rate or delta
            if (last_2_samples.filled == 2 && last_2_samples.timestamps[1] + Base::window > current_timestamp)
            {
                fillResultValue(last_2_samples.timestamps[0], last_2_samples.values[0],
                    last_2_samples.timestamps[1], last_2_samples.values[1],
                    values[i], nulls[i]);
            }
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 2;
};

}
