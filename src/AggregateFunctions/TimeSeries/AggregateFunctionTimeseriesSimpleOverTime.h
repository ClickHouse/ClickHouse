#pragma once

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>

#include <Common/DequeWithMemoryTracking.h>
#include <Common/SetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <cmath>
#include <limits>


namespace DB
{

enum class AggregateFunctionTimeseriesSimpleOverTimeKind
{
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Stddev,
    Stdvar,
    Present,
};

template <
    bool array_arguments_,
    typename TimestampType_,
    typename IntervalType_,
    typename ValueType_,
    AggregateFunctionTimeseriesSimpleOverTimeKind kind_>
struct AggregateFunctionTimeseriesSimpleOverTimeTraits
{
    static constexpr bool array_arguments = array_arguments_;
    static constexpr bool is_rate = false;
    static constexpr auto kind = kind_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Sum)
            return "timeSeriesSumOverTimeToGrid";
        else if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Avg)
            return "timeSeriesAvgOverTimeToGrid";
        else if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Min)
            return "timeSeriesMinOverTimeToGrid";
        else if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Max)
            return "timeSeriesMaxOverTimeToGrid";
        else if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Count)
            return "timeSeriesCountOverTimeToGrid";
        else if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Stddev)
            return "timeSeriesStddevOverTimeToGrid";
        else if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Stdvar)
            return "timeSeriesStdvarOverTimeToGrid";
        else
            return "timeSeriesPresentOverTimeToGrid";
    }

    struct Bucket
    {
        absl::flat_hash_map<TimestampType, ValueType> samples;

        void add(TimestampType timestamp, ValueType value)
        {
            auto it = samples.find(timestamp);
            if (it != samples.end())
                it->second = std::fmax(it->second, value);
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
class AggregateFunctionTimeseriesSimpleOverTime final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesSimpleOverTime<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;
    static constexpr auto kind = Traits::kind;

    using TimestampType = typename Traits::TimestampType;
    using ValueType = typename Traits::ValueType;
    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesSimpleOverTime<Traits>, Traits>;
    using Bucket = typename Base::Bucket;

    struct ValueLess
    {
        bool operator()(ValueType lhs, ValueType rhs) const
        {
            const bool lhs_nan = std::isnan(lhs);
            const bool rhs_nan = std::isnan(rhs);
            if (lhs_nan || rhs_nan)
                return lhs_nan && !rhs_nan;
            return lhs < rhs;
        }
    };

    using Base::Base;

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
        Float64 finite_sum = 0;
        size_t nan_count = 0;
        size_t positive_infinity_count = 0;
        size_t negative_infinity_count = 0;
        MultiSetWithMemoryTracking<ValueType, ValueLess> window_values;

        auto add_value = [&](ValueType value)
        {
            if (std::isnan(value))
            {
                ++nan_count;
            }
            else
            {
                if (std::isinf(value))
                {
                    if (value > 0)
                        ++positive_infinity_count;
                    else
                        ++negative_infinity_count;
                }
                else
                {
                    finite_sum += value;
                }

                if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Min || kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Max)
                    window_values.insert(value);
            }
        };

        auto remove_value = [&](ValueType value)
        {
            if (std::isnan(value))
            {
                --nan_count;
            }
            else
            {
                if (std::isinf(value))
                {
                    if (value > 0)
                        --positive_infinity_count;
                    else
                        --negative_infinity_count;
                }
                else
                {
                    finite_sum -= value;
                }

                if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Min || kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Max)
                {
                    auto it = window_values.find(value);
                    chassert(it != window_values.end());
                    window_values.erase(it);
                }
            }
        };

        auto sum_result = [&]() -> ValueType
        {
            if (nan_count || (positive_infinity_count && negative_infinity_count))
                return std::numeric_limits<ValueType>::quiet_NaN();
            if (positive_infinity_count)
                return std::numeric_limits<ValueType>::infinity();
            if (negative_infinity_count)
                return -std::numeric_limits<ValueType>::infinity();
            return static_cast<ValueType>(finite_sum);
        };

        auto variance_result = [&]() -> ValueType
        {
            Float64 count = 0;
            Float64 mean = 0;
            Float64 aux = 0;
            for (const auto & [timestamp, value] : samples_in_window)
            {
                (void)timestamp;
                ++count;
                const Float64 delta = value - mean;
                mean += delta / count;
                aux += delta * (value - mean);
            }

            const auto variance = aux / count;
            if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Stddev)
                return static_cast<ValueType>(std::sqrt(variance));
            else
                return static_cast<ValueType>(variance);
        };

        for (UInt32 i = 0; i < Base::bucket_count; ++i)
        {
            const TimestampType current_timestamp = Base::start_timestamp + i * Base::step;

            auto bucket_it = buckets.find(i);
            if (bucket_it != buckets.end())
            {
                timestamps_buffer.clear();
                for (const auto & [timestamp, value] : bucket_it->second.samples)
                    timestamps_buffer.emplace_back(timestamp, value);
                std::sort(timestamps_buffer.begin(), timestamps_buffer.end());

                for (const auto & [timestamp, value] : timestamps_buffer)
                {
                    samples_in_window.push_back({timestamp, value});
                    add_value(value);
                }
            }

            while (!samples_in_window.empty() && samples_in_window.front().first + Base::window <= current_timestamp)
            {
                const ValueType removed_value = samples_in_window.front().second;
                samples_in_window.pop_front();
                remove_value(removed_value);
            }

            if (samples_in_window.empty())
            {
                values[i] = 0;
                nulls[i] = 1;
                continue;
            }

            if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Sum)
                values[i] = sum_result();
            else if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Avg)
                values[i] = static_cast<ValueType>(sum_result() / static_cast<Float64>(samples_in_window.size()));
            else if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Min)
                values[i] = window_values.empty() ? std::numeric_limits<ValueType>::quiet_NaN() : *window_values.begin();
            else if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Max)
                values[i] = window_values.empty() ? std::numeric_limits<ValueType>::quiet_NaN() : *window_values.rbegin();
            else if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Count)
                values[i] = static_cast<ValueType>(samples_in_window.size());
            else if constexpr (kind == AggregateFunctionTimeseriesSimpleOverTimeKind::Present)
                values[i] = 1;
            else
                values[i] = variance_result();

            nulls[i] = 0;
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 1;
};

}
