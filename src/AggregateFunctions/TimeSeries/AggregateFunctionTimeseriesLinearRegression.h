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

template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_predict_>
struct AggregateFunctionTimeseriesLinearRegressionTraits
{
    static constexpr bool array_arguments = array_arguments_;
    static constexpr bool is_predict = is_predict_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return is_predict ? "timeSeriesPredictLinearToGrid" : "timeSeriesDerivToGrid";
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
class AggregateFunctionTimeseriesLinearRegression final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesLinearRegression<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    static constexpr bool is_predict = Traits::is_predict;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesLinearRegression<Traits>, Traits>;

    using Base::Base;

    using Bucket = typename Base::Bucket;

    explicit AggregateFunctionTimeseriesLinearRegression(const DataTypes & argument_types_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_, UInt32 timestamp_scale_, Float64 predict_offset_ = 0)
        : Base(argument_types_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , predict_offset(predict_offset_)
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
    std::pair<Float64, Float64> kahanSumInc(Float64 inc, Float64 sum, Float64 c) const
    {
        Float64 new_sum = sum + inc;

        // Using Neumaier improvement, swap if next term larger than sum.
        if (std::abs(sum) >= std::abs(inc))
            c += (sum - new_sum) + inc;
        else
            c += (inc - new_sum) + sum;

        return std::make_pair(new_sum, c);
    }

    void fillResultValue(const TimestampType current_timestamp,
        const std::deque<std::pair<TimestampType, ValueType>> & samples_in_window,
        ValueType & result, UInt8 & null) const
    {
        size_t n = samples_in_window.size();
        if (n < 2)
        {
            result = 0;
            null = 1;
            return;
        }

        const TimestampType first_timestamp = samples_in_window.front().first;
        const TimestampType last_timestamp = samples_in_window.back().first;

        const TimestampType time_difference = last_timestamp - first_timestamp;
        if (time_difference == 0)
        {
            result = 0;
            null = 1;
            return;
        }

        // The following least-square linear regression logic is copied from Prometheus:
        // https://github.com/prometheus/prometheus/blob/9a0bbb60bc3eb68d045aae7535d34f4d02b959f1/promql/functions.go#L1209
        const TimestampType intercept_time = is_predict ? current_timestamp : first_timestamp;
        Float64 sum_x = 0;
        Float64 c_x = 0;
        Float64 sum_y = 0;
        Float64 c_y = 0;
        Float64 sum_xy = 0;
        Float64 c_xy = 0;
        Float64 sum_xx = 0;
        Float64 c_xx = 0;

        for (const auto& sample : samples_in_window)
        {
            Float64 x = static_cast<Float64>(sample.first) - static_cast<Float64>(intercept_time);
            Float64 y = static_cast<Float64>(sample.second);
            std::tie(sum_x, c_x) = kahanSumInc(x, sum_x, c_x);
            std::tie(sum_y, c_y) = kahanSumInc(y, sum_y, c_y);
            std::tie(sum_xy, c_xy) = kahanSumInc(x * y, sum_xy, c_xy);
            std::tie(sum_xx, c_xx) = kahanSumInc(x * x, sum_xx, c_xx);
        }
        sum_x += c_x;
        sum_y += c_y;
        sum_xy += c_xy;
        sum_xx += c_xx;

        Float64 cov_xy = sum_xy - sum_x * sum_y / n;
        Float64 var_x = sum_xx - sum_x * sum_x / n;

        Float64 slope = cov_xy / var_x;
        if (is_predict)
        {
            Float64 intercept = sum_y / n - slope * sum_x / n;
            Float64 predicted_value = slope * predict_offset + intercept;
            result = static_cast<ValueType>(predicted_value);
        }
        else
        {
            result = static_cast<ValueType>(slope);
        }
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
                current_timestamp,
                samples_in_window,
                values[i],
                nulls[i]);
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 1;

protected:
    const Float64 predict_offset{};    /// Predict offset used by timeSeriesPredictLinearToGrid function, used to calculate the timestamp of the predicted value
};

}
