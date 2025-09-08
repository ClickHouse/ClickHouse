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
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>


namespace DB
{

template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_rate_>
struct AggregateFunctionTimeseriesToGridSparseTraits
{
    static constexpr bool array_arguments = array_arguments_;
    static constexpr bool is_rate = is_rate_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return "timeSeriesResampleToGridWithStaleness";
    }

    struct Bucket
    {
        TimestampType first = 0;
        ValueType second = 0;

        void add(TimestampType timestamp, ValueType value)
        {
            if (timestamp > first || (timestamp == first && value > second))
            {
                first = timestamp;
                second = value;
            }
        }

        void merge(const Bucket & other)
        {
            add(other.first, other.second);
        }
    };
};


/// Aggregate function to convert timeseries to the specified grid with staleness
/// Missing values are filled with NULLs
template <typename Traits>
class AggregateFunctionTimeseriesToGridSparse final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesToGridSparse<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    static_assert(Traits::is_rate == false, "AggregateFunctionTimeseriesToGridSparse does not have rate version");

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesToGridSparse<Traits>, Traits>;

    using Base::Base;

    using Bucket = typename Base::Bucket;

    static void serializeBucket(const Bucket & bucket, WriteBuffer & buf)
    {
        writeBinaryLittleEndian(bucket.first, buf);
        writeBinaryLittleEndian(bucket.second, buf);
    }

    void deserializeBucket(Bucket & bucket, ReadBuffer & buf, const size_t bucket_index) const
    {
        TimestampType timestamp;
        readBinaryLittleEndian(timestamp, buf);
        Base::checkTimestampInRange(timestamp, bucket_index);

        ValueType value;
        readBinaryLittleEndian(value, buf);

        bucket = {timestamp, value};
    }

    /// Insert the result into the column
    void doInsertResultInto(AggregateDataPtr __restrict place, IColumn & to) const
    {
        std::vector<TimestampType> timestamps;

        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.empty() ? Base::bucket_count : offsets_to.back() + Base::bucket_count);

        if (!Base::bucket_count)
            return;

        ColumnNullable & result_to = typeid_cast<ColumnNullable &>(arr_to.getData());
        auto & data_to = typeid_cast<typename Base::ColVecResultType &>(result_to.getNestedColumn()).getData();
        auto & nulls_to = result_to.getNullMapData();

        const size_t old_size = data_to.size();
        chassert(old_size == nulls_to.size(), "Sizes of nested column and null map of Nullable column are not equal");

        data_to.resize(old_size + Base::bucket_count);
        nulls_to.resize(old_size + Base::bucket_count);

        ValueType * values = data_to.data() + old_size;  /// use result column as a buffer for values
        UInt8 * nulls = nulls_to.data() + old_size;

        /// Fill all timestamps with zeros to indicate missing values
        timestamps.assign(Base::bucket_count, 0);
        std::fill(nulls, nulls + Base::bucket_count, 1);

        /// Fill the data for existing buckets
        for (const auto & bucket : Base::data(place)->buckets)
        {
            timestamps[bucket.first] = bucket.second.first;
            values[bucket.first] = bucket.second.second;
            nulls[bucket.first] = 0;
        }

        /// Fill the data for missing buckets
        TimestampType current_timestamp = Base::start_timestamp;

        bool has_previous_value = false;
        ValueType previous_value = {};
        TimestampType previous_timestamp = {};

        for (size_t i = 0; i < Base::bucket_count; ++i, current_timestamp += Base::step)
        {
            /// Current bucket has a value?
            if (!nulls[i])
            {
                has_previous_value = true;
                previous_value = values[i];
                previous_timestamp = timestamps[i];
            }
            else if (has_previous_value && (previous_timestamp + Base::window >= current_timestamp))
            {
                /// Use the previous value if the current timestamp is missing and the previous one is not stale
                values[i] = previous_value;
                nulls[i] = 0;
            }
            else
            {
                values[i] = ValueType{};
            }
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 2;
};

}
