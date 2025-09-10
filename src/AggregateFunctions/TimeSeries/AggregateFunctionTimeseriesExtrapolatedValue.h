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

template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_rate_>
struct AggregateFunctionTimeseriesExtrapolatedValueTraits
{
    static constexpr bool array_arguments = array_arguments_;
    static constexpr bool is_rate = is_rate_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return is_rate ? "timeSeriesRateToGrid" : "timeSeriesDeltaToGrid";
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

/// Aggregate function to calculate extrapolated values (rate and delta) of timeseries on the specified grid
template <typename Traits>
class AggregateFunctionTimeseriesExtrapolatedValue final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesExtrapolatedValue<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    static constexpr bool is_rate = Traits::is_rate;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesExtrapolatedValue<Traits>, Traits>;

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
    void fillResultValue(const TimestampType current_timestamp,
        const std::deque<std::pair<TimestampType, ValueType>> & samples_in_window,
        Float64 accumulated_resets_in_window,
        ValueType & result, UInt8 & null) const
    {
        /// Need at least two samples to calculate the rate or delta
        if (samples_in_window.size() < 2)
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

        const ValueType first_value = samples_in_window.front().second;
        const ValueType last_value = samples_in_window.back().second;

        Float64 value_difference = last_value - first_value + accumulated_resets_in_window;

        const auto range_end = current_timestamp;
        const auto range_start = current_timestamp - Base::window;

        /// The following logic is copied from Prometheus' rate calculation
        /// https://github.com/prometheus/prometheus/blob/5e124cf4f2b9467e4ae1c679840005e727efd599/promql/functions.go#L127
        /// which is licensed under the Apache License 2.0
        // Duration between first/last samples and boundary of range.
        Float64 duration_to_start = first_timestamp - range_start;
        Float64 duration_to_end = range_end - last_timestamp;

        const auto sampled_interval = time_difference;
        const Float64 average_duration_between_samples = sampled_interval / Float64(samples_in_window.size() - 1);

        // If samples are close enough to the (lower or upper) boundary of the
        // range, we extrapolate the rate all the way to the boundary in
        // question. "Close enough" is defined as "up to 10% more than the
        // average duration between samples within the range", see
        // extrapolationThreshold below. Essentially, we are assuming a more or
        // less regular spacing between samples, and if we don't see a sample
        // where we would expect one, we assume the series does not cover the
        // whole range, but starts and/or ends within the range. We still
        // extrapolate the rate in this case, but not all the way to the
        // boundary, but only by half of the average duration between samples
        // (which is our guess for where the series actually starts or ends).

        const auto extrapolation_threshold = average_duration_between_samples * 1.1;
        Float64 extrapolate_to_interval = sampled_interval;

        if (duration_to_start >= extrapolation_threshold)
            duration_to_start = average_duration_between_samples / 2;

        if (is_rate && value_difference > 0 && first_value >= 0)
        {
            // Counters cannot be negative. If we have any slope at all
            // (i.e. resultFloat went up), we can extrapolate the zero point
            // of the counter. If the duration to the zero point is shorter
            // than the durationToStart, we take the zero point as the start
            // of the series, thereby avoiding extrapolation to negative
            // counter values.
            Float64 duration_to_zero = sampled_interval * (first_value / value_difference);
            duration_to_start = std::min(duration_to_zero, duration_to_start);
        }

        extrapolate_to_interval += duration_to_start;

        if (duration_to_end >= extrapolation_threshold)
            duration_to_end = average_duration_between_samples / 2;
        extrapolate_to_interval += duration_to_end;

        Float64 factor = extrapolate_to_interval / sampled_interval;

        if constexpr (is_rate)
            factor = factor * Base::timestamp_scale_multiplier / Base::window;

        value_difference *= factor;

        result = static_cast<ValueType>(value_difference);
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
        Float64 accumulated_resets_in_window = 0;

        /// Resets must be take into account for `rate` function because it expects counter timeseries that only increase.
        /// But `delta` function expects gauge timeseries that can decrease and it is not considered to be a reset.
        constexpr bool adjust_to_resets = is_rate;

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
                {
                    /// Check for resets in the timeseries
                    if (adjust_to_resets && !samples_in_window.empty() && samples_in_window.back().second > value)
                        accumulated_resets_in_window += samples_in_window.back().second;
                    samples_in_window.push_back({timestamp, value});
                }
            }

            /// Remove samples that are out of the window
            while (!samples_in_window.empty() && samples_in_window.front().first + Base::window <= current_timestamp)
            {
                Float64 removed_value = samples_in_window.front().second;
                samples_in_window.pop_front();
                /// Subtract resets that are out of the window
                if (adjust_to_resets && !samples_in_window.empty() && samples_in_window.front().second < removed_value)
                    accumulated_resets_in_window -= removed_value;
            }

            fillResultValue(
                current_timestamp,
                samples_in_window,
                accumulated_resets_in_window,
                values[i],
                nulls[i]);
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 2;
};

}
