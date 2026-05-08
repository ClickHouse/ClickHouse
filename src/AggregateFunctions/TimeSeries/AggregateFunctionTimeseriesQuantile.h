#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <limits>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <DataTypes/DataTypesDecimal.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>

#include <absl/container/flat_hash_map.h>


namespace DB
{

template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_>
struct AggregateFunctionTimeseriesQuantileTraits
{
    static constexpr bool array_arguments = array_arguments_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return "timeSeriesQuantileToGrid";
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
            samples.reserve(samples.size() + other.samples.size());

            for (const auto & [timestamp, value] : other.samples)
                add(timestamp, value);
        }
    };
};

template <typename Traits>
class AggregateFunctionTimeseriesQuantile final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesQuantile<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesQuantile<Traits>, Traits>;

    using Bucket = typename Base::Bucket;

    explicit AggregateFunctionTimeseriesQuantile(
        const DataTypes & argument_types_,
        TimestampType start_timestamp_,
        TimestampType end_timestamp_,
        IntervalType step_,
        IntervalType window_,
        UInt32 timestamp_scale_,
        Float64 phi_)
        : Base(argument_types_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , phi(phi_)
    {
    }

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
        readBinaryLittleEndian(sample_count, buf);
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
    static bool valueLess(ValueType lhs, ValueType rhs)
    {
        const bool lhs_nan = std::isnan(lhs);
        const bool rhs_nan = std::isnan(rhs);
        if (lhs_nan || rhs_nan)
            return lhs_nan && !rhs_nan;
        return lhs < rhs;
    }

    void fillResultValue(
        const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window,
        VectorWithMemoryTracking<ValueType> & values_buffer,
        ValueType & result,
        UInt8 & null) const
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        if (std::isnan(phi))
        {
            result = std::numeric_limits<ValueType>::quiet_NaN();
            null = 0;
            return;
        }
        if (phi < 0)
        {
            result = -std::numeric_limits<ValueType>::infinity();
            null = 0;
            return;
        }
        if (phi > 1)
        {
            result = std::numeric_limits<ValueType>::infinity();
            null = 0;
            return;
        }

        values_buffer.clear();
        values_buffer.reserve(samples_in_window.size());
        for (const auto & sample : samples_in_window)
            values_buffer.push_back(sample.second);

        std::sort(values_buffer.begin(), values_buffer.end(), valueLess);

        const Float64 n = static_cast<Float64>(values_buffer.size());
        const Float64 rank = phi * (n - 1);
        const auto lower_index = static_cast<size_t>(std::max(0.0, std::floor(rank)));
        const auto upper_index = static_cast<size_t>(std::min(n - 1, static_cast<Float64>(lower_index + 1)));
        const Float64 weight = rank - std::floor(rank);

        result = static_cast<ValueType>(
            static_cast<Float64>(values_buffer[lower_index]) * (1 - weight)
            + static_cast<Float64>(values_buffer[upper_index]) * weight);
        null = 0;
    }

public:
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

        DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> samples_in_window;
        VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> timestamps_buffer;
        VectorWithMemoryTracking<ValueType> values_buffer;

        /// Fill the data for missing buckets.
        for (UInt32 i = 0; i < Base::bucket_count; ++i)
        {
            const TimestampType current_timestamp = Base::start_timestamp + i * Base::step;

            auto bucket_it = buckets.find(i);
            if (bucket_it != buckets.end())
            {
                timestamps_buffer.clear();
                /// Sort samples from the current bucket.
                for (const auto & [timestamp, value] : bucket_it->second.samples)
                    timestamps_buffer.emplace_back(timestamp, value);
                std::sort(timestamps_buffer.begin(), timestamps_buffer.end());

                /// Add samples from the current bucket.
                for (const auto & [timestamp, value] : timestamps_buffer)
                    samples_in_window.push_back({timestamp, value});
            }

            /// Remove samples that are out of the window.
            while (!samples_in_window.empty() && samples_in_window.front().first + Base::window <= current_timestamp)
                samples_in_window.pop_front();

            fillResultValue(
                samples_in_window,
                values_buffer,
                values[i],
                nulls[i]);
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 1;

protected:
    const Float64 phi{};
};

}
