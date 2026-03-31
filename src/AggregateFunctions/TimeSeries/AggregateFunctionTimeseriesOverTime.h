#pragma once

#include <cstddef>
#include <cstring>
#include <algorithm>
#include <cmath>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>

#include <absl/container/flat_hash_map.h>


namespace DB
{

enum class OverTimeKind : UInt8
{
    Avg,
    Min,
    Max,
    Sum,
    Count,
    StddevPop,
    StdvarPop,
    Present,
    Absent,
    First,
    TsOfLast,
    TsOfFirst,
    TsOfMin,
    TsOfMax,
    Quantile,
    Mad,
};


template <OverTimeKind kind_>
struct OverTimeKindInfo
{
    static String getName()
    {
        if constexpr (kind_ == OverTimeKind::Avg) return "timeSeriesAvgOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::Min) return "timeSeriesMinOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::Max) return "timeSeriesMaxOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::Sum) return "timeSeriesSumOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::Count) return "timeSeriesCountOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::StddevPop) return "timeSeriesStddevOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::StdvarPop) return "timeSeriesStdvarOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::Present) return "timeSeriesPresentOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::Absent) return "timeSeriesAbsentOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::First) return "timeSeriesFirstOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::TsOfLast) return "timeSeriesTsOfLastOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::TsOfFirst) return "timeSeriesTsOfFirstOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::TsOfMin) return "timeSeriesTsOfMinOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::TsOfMax) return "timeSeriesTsOfMaxOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::Quantile) return "timeSeriesQuantileOverTimeToGrid";
        else if constexpr (kind_ == OverTimeKind::Mad) return "timeSeriesMadOverTimeToGrid";
    }
};


/// The Traits template stores samples per bucket in a flat_hash_map, following the same
/// pattern as ExtrapolatedValue, Changes, and LinearRegression.
/// The OverTimeKind only affects result computation in doInsertResultInto, not the bucket structure.
template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_, OverTimeKind kind_>
struct AggregateFunctionTimeseriesOverTimeTraitsBase
{
    static constexpr bool array_arguments = array_arguments_;
    static constexpr OverTimeKind kind = kind_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return OverTimeKindInfo<kind_>::getName();
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


/// Template aliases matching the 5-parameter template template signature required by
/// createAggregateFunctionTimeseries. The 5th bool parameter is unused for _over_time functions.
#define DEFINE_OVER_TIME_TRAITS_ALIAS(KIND) \
    template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_, bool /*unused*/> \
    using AggregateFunctionTimeseries##KIND##OverTimeTraits = \
        AggregateFunctionTimeseriesOverTimeTraitsBase<array_arguments_, TimestampType_, IntervalType_, ValueType_, OverTimeKind::KIND>;

DEFINE_OVER_TIME_TRAITS_ALIAS(Avg)
DEFINE_OVER_TIME_TRAITS_ALIAS(Min)
DEFINE_OVER_TIME_TRAITS_ALIAS(Max)
DEFINE_OVER_TIME_TRAITS_ALIAS(Sum)
DEFINE_OVER_TIME_TRAITS_ALIAS(Count)
DEFINE_OVER_TIME_TRAITS_ALIAS(StddevPop)
DEFINE_OVER_TIME_TRAITS_ALIAS(StdvarPop)
DEFINE_OVER_TIME_TRAITS_ALIAS(Present)
DEFINE_OVER_TIME_TRAITS_ALIAS(Absent)
DEFINE_OVER_TIME_TRAITS_ALIAS(First)
DEFINE_OVER_TIME_TRAITS_ALIAS(TsOfLast)
DEFINE_OVER_TIME_TRAITS_ALIAS(TsOfFirst)
DEFINE_OVER_TIME_TRAITS_ALIAS(TsOfMin)
DEFINE_OVER_TIME_TRAITS_ALIAS(TsOfMax)
DEFINE_OVER_TIME_TRAITS_ALIAS(Quantile)
DEFINE_OVER_TIME_TRAITS_ALIAS(Mad)

#undef DEFINE_OVER_TIME_TRAITS_ALIAS


template <typename Traits>
class AggregateFunctionTimeseriesOverTime final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTime<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;
    static constexpr OverTimeKind kind = Traits::kind;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTime<Traits>, Traits>;
    using Base::Base;
    using Bucket = typename Base::Bucket;

    explicit AggregateFunctionTimeseriesOverTime(const DataTypes & argument_types_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_,
        UInt32 timestamp_scale_, Float64 extra_param_ = 0)
        : Base(argument_types_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , extra_param(extra_param_)
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
    void fillResultValue(
        const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window,
        ValueType & result, UInt8 & null) const
    {
        if constexpr (kind == OverTimeKind::Absent)
        {
            if (samples_in_window.empty())
            {
                result = static_cast<ValueType>(1);
                null = 0;
            }
            else
            {
                result = 0;
                null = 1;
            }
            return;
        }

        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        if constexpr (kind == OverTimeKind::Avg)
        {
            Float64 sum = 0;
            for (const auto & [ts, val] : samples_in_window)
                sum += static_cast<Float64>(val);
            result = static_cast<ValueType>(sum / static_cast<Float64>(samples_in_window.size()));
        }
        else if constexpr (kind == OverTimeKind::Min)
        {
            ValueType min_val = samples_in_window.front().second;
            for (const auto & [ts, val] : samples_in_window)
                min_val = std::min(min_val, val);
            result = min_val;
        }
        else if constexpr (kind == OverTimeKind::Max)
        {
            ValueType max_val = samples_in_window.front().second;
            for (const auto & [ts, val] : samples_in_window)
                max_val = std::max(max_val, val);
            result = max_val;
        }
        else if constexpr (kind == OverTimeKind::Sum)
        {
            Float64 sum = 0;
            for (const auto & [ts, val] : samples_in_window)
                sum += static_cast<Float64>(val);
            result = static_cast<ValueType>(sum);
        }
        else if constexpr (kind == OverTimeKind::Count)
        {
            result = static_cast<ValueType>(samples_in_window.size());
        }
        else if constexpr (kind == OverTimeKind::StddevPop || kind == OverTimeKind::StdvarPop)
        {
            Float64 sum = 0;
            Float64 sum_sq = 0;
            size_t n = samples_in_window.size();
            for (const auto & [ts, val] : samples_in_window)
            {
                Float64 v = static_cast<Float64>(val);
                sum += v;
                sum_sq += v * v;
            }
            Float64 mean = sum / static_cast<Float64>(n);
            Float64 variance = std::max(0.0, sum_sq / static_cast<Float64>(n) - mean * mean);
            if constexpr (kind == OverTimeKind::StdvarPop)
                result = static_cast<ValueType>(variance);
            else
                result = static_cast<ValueType>(std::sqrt(variance));
        }
        else if constexpr (kind == OverTimeKind::Present)
        {
            result = static_cast<ValueType>(1);
        }
        else if constexpr (kind == OverTimeKind::First)
        {
            result = samples_in_window.front().second;
        }
        else if constexpr (kind == OverTimeKind::TsOfLast)
        {
            Float64 ts_sec = static_cast<Float64>(samples_in_window.back().first)
                             / static_cast<Float64>(Base::timestamp_scale_multiplier);
            result = static_cast<ValueType>(ts_sec);
        }
        else if constexpr (kind == OverTimeKind::TsOfFirst)
        {
            Float64 ts_sec = static_cast<Float64>(samples_in_window.front().first)
                             / static_cast<Float64>(Base::timestamp_scale_multiplier);
            result = static_cast<ValueType>(ts_sec);
        }
        else if constexpr (kind == OverTimeKind::TsOfMin)
        {
            auto best = samples_in_window.begin();
            for (auto it = samples_in_window.begin(); it != samples_in_window.end(); ++it)
                if (it->second < best->second)
                    best = it;
            Float64 ts_sec = static_cast<Float64>(best->first)
                             / static_cast<Float64>(Base::timestamp_scale_multiplier);
            result = static_cast<ValueType>(ts_sec);
        }
        else if constexpr (kind == OverTimeKind::TsOfMax)
        {
            auto best = samples_in_window.begin();
            for (auto it = samples_in_window.begin(); it != samples_in_window.end(); ++it)
                if (it->second > best->second)
                    best = it;
            Float64 ts_sec = static_cast<Float64>(best->first)
                             / static_cast<Float64>(Base::timestamp_scale_multiplier);
            result = static_cast<ValueType>(ts_sec);
        }
        else if constexpr (kind == OverTimeKind::Quantile)
        {
            VectorWithMemoryTracking<Float64> sorted_values;
            sorted_values.reserve(samples_in_window.size());
            for (const auto & [ts, val] : samples_in_window)
                sorted_values.push_back(static_cast<Float64>(val));
            std::sort(sorted_values.begin(), sorted_values.end());

            Float64 phi = std::clamp(extra_param, 0.0, 1.0);
            Float64 rank = phi * static_cast<Float64>(sorted_values.size() - 1);
            size_t lower_index = static_cast<size_t>(std::floor(rank));
            size_t upper_index = std::min(static_cast<size_t>(std::ceil(rank)), sorted_values.size() - 1);
            Float64 fraction = rank - static_cast<Float64>(lower_index);
            result = static_cast<ValueType>(sorted_values[lower_index] * (1.0 - fraction) + sorted_values[upper_index] * fraction);
        }
        else if constexpr (kind == OverTimeKind::Mad)
        {
            VectorWithMemoryTracking<Float64> vals;
            vals.reserve(samples_in_window.size());
            for (const auto & [ts, val] : samples_in_window)
                vals.push_back(static_cast<Float64>(val));
            std::sort(vals.begin(), vals.end());

            size_t n = vals.size();
            Float64 median;
            if (n % 2 == 0)
                median = (vals[n / 2 - 1] + vals[n / 2]) / 2.0;
            else
                median = vals[n / 2];

            VectorWithMemoryTracking<Float64> abs_devs;
            abs_devs.reserve(n);
            for (Float64 v : vals)
                abs_devs.push_back(std::abs(v - median));
            std::sort(abs_devs.begin(), abs_devs.end());

            Float64 mad;
            if (n % 2 == 0)
                mad = (abs_devs[n / 2 - 1] + abs_devs[n / 2]) / 2.0;
            else
                mad = abs_devs[n / 2];
            result = static_cast<ValueType>(mad);
        }

        null = 0;
    }

public:
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
                    samples_in_window.push_back({timestamp, value});
            }

            while (!samples_in_window.empty() && samples_in_window.front().first + Base::window <= current_timestamp)
                samples_in_window.pop_front();

            fillResultValue(samples_in_window, values[i], nulls[i]);
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 1;

protected:
    const Float64 extra_param{};
};

}
