#pragma once

#include <cstddef>
#include <cstring>
#include <deque>
#include <vector>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>

#include <absl/container/flat_hash_map.h>


namespace DB
{

template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_resets_>
struct AggregateFunctionTimeseriesChangesTraits
{
    static constexpr bool array_arguments = array_arguments_;
    static constexpr bool is_resets = is_resets_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return is_resets ? "timeSeriesResetsToGrid" : "timeSeriesChangesToGrid";
    }

    struct Bucket
    {
        absl::flat_hash_map<TimestampType, ValueType> samples;

        void add(TimestampType timestamp, ValueType value)
        {
            auto it = samples.find(timestamp);
            if (it != samples.end())
                it->second = std::max(it->second, value);
            else
                samples[timestamp] = value;
        }

        void merge(const Bucket & other)
        {
            samples.reserve(other.samples.size());

            for (const auto & [timestamp, value] : other.samples)
                add(timestamp, value);
        }
    };
};

template <typename Traits>
class AggregateFunctionTimeseriesChanges final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesChanges<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    static constexpr bool is_resets = Traits::is_resets;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesChanges<Traits>, Traits>;

    using Base::Base;

    using Bucket = typename Base::Bucket;

    static void serializeBucket(const Bucket & bucket, WriteBuffer & buf)
    {
        writeBinaryLittleEndian(bucket.samples.size(), buf);
        for (const auto & sample : bucket.samples)
        {
            writeBinaryLittleEndian(sample.first, buf);
            writeBinaryLittleEndian(sample.second, buf);
        }
    }

    void deserializeBucket(Bucket & bucket, ReadBuffer & buf, const size_t bucket_index) const
    {
        size_t sample_count;
        readBinaryLittleEndian(sample_count,buf);
        bucket.samples.reserve(sample_count);

        for (size_t s = 0; s < sample_count; ++s)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            Base::checkTimestampInRange(timestamp, bucket_index);

            ValueType value;
            readBinaryLittleEndian(value, buf);

            bucket.add(timestamp, value);
        }
    }

private:
    void fillResultValue(const std::deque<std::pair<TimestampType, ValueType>> & samples_in_window,
        ValueType & result, UInt8 & null) const
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        UInt64 count = 0;
        ValueType prev_sample_value = samples_in_window.front().second;
        for (const auto& sample : samples_in_window)
        {
            if constexpr (is_resets)
            {
                bool is_reset = (sample.second < prev_sample_value);
                if (is_reset)
                    count++;
            }
            else
            {
                bool is_change = (sample.second != prev_sample_value);
                if (is_change)
                    count++;
            }
            prev_sample_value = sample.second;
        }

        result = static_cast<ValueType>(count);
        null = 0;
    }

public:
    /// Insert the result into the column
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

        std::deque<std::pair<TimestampType, ValueType>> samples_in_window;
        std::vector<std::pair<TimestampType, ValueType>> timestamps_buffer;


        /// Fill the data for missing buckets
        for (UInt32 i = 0; i < Base::bucket_count; ++i)
        {
            const TimestampType current_timestamp = Base::start_timestamp + i * Base::step;

            auto bucket_it = buckets.find(i);
            if (bucket_it != buckets.end())
            {
                timestamps_buffer.clear();
                /// Sort samples from the current bucket
                for (const auto & [timestamp, value] : bucket_it->second.samples)
                    timestamps_buffer.emplace_back(timestamp, value);
                std::sort(timestamps_buffer.begin(), timestamps_buffer.end());

                /// Add samples from the current bucket
                for (const auto & [timestamp, value] : timestamps_buffer)
                    samples_in_window.push_back({timestamp, value});
            }

            /// Remove samples that are out of the window
            while (!samples_in_window.empty() && samples_in_window.front().first + Base::window <= current_timestamp)
            {
                samples_in_window.pop_front();
            }

            fillResultValue(
                samples_in_window,
                values[i],
                nulls[i]);
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 1;
};

}
