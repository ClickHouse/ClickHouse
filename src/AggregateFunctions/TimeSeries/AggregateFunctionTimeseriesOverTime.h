#pragma once

#include <cstddef>
#include <cstring>
#include <algorithm>
#include <cmath>
#include <limits>

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

/// Per-output-bucket storage layout.
/// Duplicate timestamps within a bucket are merged by max(value).
enum class OverTimeBucketKind : UInt8
{
    /// All (timestamp, value) pairs stored in a hash map. Used by aggregates that need every sample
    /// (avg, sum, count, stddev, quantile, …).
    Map,
    /// Same logical content as Map but in a sorted unique vector, allowing O(n) merge and direct
    /// push into the sliding-window deque. Used by min, max, first, ts_of_first, ts_of_min, ts_of_max:
    /// these need all samples in the window because the relevant sample (e.g. the minimum) may have
    /// an earlier timestamp than the latest sample in the bucket and could expire while other samples
    /// from the same bucket remain in the window.
    SortedUnique,
    /// Stores only the single sample with the maximum timestamp per bucket. When that sample expires
    /// from the sliding window, all other samples in the bucket (which have ts ≤ max_ts) are
    /// guaranteed to have expired too. Used by present_over_time, absent_over_time, ts_of_last_over_time.
    SingleSample,
};

/// Mirrors Prometheus `promql/quantile.go` func quantile(q float64, values vectorByValueHeap) float64
/// https://github.com/prometheus/prometheus/blob/da1f89e7360a19c5de2b0df4b43411ac706a76a9/promql/quantile.go#L716-L745
/// Sorts `values` in place when returning an interpolated quantile (same as Go sort.Sort before indexing).
inline Float64 quantile(Float64 q, VectorWithMemoryTracking<Float64> & values)
{
    if (values.empty() || std::isnan(q))
        return std::numeric_limits<Float64>::quiet_NaN();
    if (q < 0)
        return -std::numeric_limits<Float64>::infinity();
    if (q > 1)
        return std::numeric_limits<Float64>::infinity();

    std::sort(values.begin(), values.end());

    const Float64 n = static_cast<Float64>(values.size());
    const Float64 rank = q * (n - 1.0);
    const Float64 lower_index_float = std::max(0.0, std::floor(rank));
    const size_t lower_index = static_cast<size_t>(lower_index_float);
    const size_t upper_index = std::min(values.size() - 1, lower_index + 1);
    const Float64 weight = rank - std::floor(rank);
    return values[lower_index] * (1.0 - weight) + values[upper_index] * weight;
}

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesAvgOverTimeOperation
{
    static String getName() { return "timeSeriesAvgOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        Float64 sum = 0;
        for (const auto & [ts, val] : samples_in_window)
            sum += static_cast<Float64>(val);

        result = static_cast<ValueType>(sum / static_cast<Float64>(samples_in_window.size()));
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesMinOverTimeOperation
{
    static String getName() { return "timeSeriesMinOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        ValueType min_val = samples_in_window.front().second;
        for (const auto & [ts, val] : samples_in_window)
            min_val = std::min(min_val, val);
        result = min_val;
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesMaxOverTimeOperation
{
    static String getName() { return "timeSeriesMaxOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        ValueType max_val = samples_in_window.front().second;
        for (const auto & [ts, val] : samples_in_window)
            max_val = std::max(max_val, val);
        result = max_val;
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesSumOverTimeOperation
{
    static String getName() { return "timeSeriesSumOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        Float64 sum = 0;
        for (const auto & [ts, val] : samples_in_window)
            sum += static_cast<Float64>(val);
        result = static_cast<ValueType>(sum);
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesCountOverTimeOperation
{
    static String getName() { return "timeSeriesCountOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        result = static_cast<ValueType>(samples_in_window.size());
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesStddevPopOverTimeOperation
{
    static String getName() { return "timeSeriesStddevOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

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
        result = static_cast<ValueType>(std::sqrt(variance));
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesStdvarPopOverTimeOperation
{
    static String getName() { return "timeSeriesStdvarOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

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
        result = static_cast<ValueType>(variance);
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesPresentOverTimeOperation
{
    static String getName() { return "timeSeriesPresentOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        result = static_cast<ValueType>(1);
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesAbsentOverTimeOperation
{
    static String getName() { return "timeSeriesAbsentOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType)
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
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesFirstOverTimeOperation
{
    static String getName() { return "timeSeriesFirstOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        result = samples_in_window.front().second;
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesTsOfLastOverTimeOperation
{
    static String getName() { return "timeSeriesTsOfLastOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType timestamp_scale_multiplier)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        Float64 ts_sec = static_cast<Float64>(samples_in_window.back().first) / static_cast<Float64>(timestamp_scale_multiplier);
        result = static_cast<ValueType>(ts_sec);
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesTsOfFirstOverTimeOperation
{
    static String getName() { return "timeSeriesTsOfFirstOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType timestamp_scale_multiplier)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        Float64 ts_sec = static_cast<Float64>(samples_in_window.front().first) / static_cast<Float64>(timestamp_scale_multiplier);
        result = static_cast<ValueType>(ts_sec);
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesTsOfMinOverTimeOperation
{
    static String getName() { return "timeSeriesTsOfMinOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType timestamp_scale_multiplier)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        auto best = samples_in_window.begin();
        for (auto it = samples_in_window.begin(); it != samples_in_window.end(); ++it)
        {
            if (it->second < best->second)
                best = it;
        }

        Float64 ts_sec = static_cast<Float64>(best->first) / static_cast<Float64>(timestamp_scale_multiplier);
        result = static_cast<ValueType>(ts_sec);
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesTsOfMaxOverTimeOperation
{
    static String getName() { return "timeSeriesTsOfMaxOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType timestamp_scale_multiplier)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        auto best = samples_in_window.begin();
        for (auto it = samples_in_window.begin(); it != samples_in_window.end(); ++it)
        {
            if (it->second > best->second)
                best = it;
        }

        Float64 ts_sec = static_cast<Float64>(best->first) / static_cast<Float64>(timestamp_scale_multiplier);
        result = static_cast<ValueType>(ts_sec);
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesQuantileOverTimeOperation
{
    static String getName() { return "timeSeriesQuantileOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64 extra_param, TimestampType)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        /// Match Prometheus `quantile()` used by quantile_over_time (including phi edge cases):
        /// https://github.com/prometheus/prometheus/blob/da1f89e7360a19c5de2b0df4b43411ac706a76a9/promql/quantile.go#L716-L745
        VectorWithMemoryTracking<Float64> values;
        values.reserve(samples_in_window.size());
        for (const auto & [ts, val] : samples_in_window)
            values.push_back(static_cast<Float64>(val));

        const Float64 phi = extra_param;
        result = static_cast<ValueType>(quantile(phi, values));
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesMadOverTimeOperation
{
    static String getName() { return "timeSeriesMadOverTimeToGrid"; }
    static void fillResultValue(const DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (samples_in_window.empty())
        {
            result = 0;
            null = 1;
            return;
        }

        VectorWithMemoryTracking<Float64> vals;
        vals.reserve(samples_in_window.size());
        for (const auto & [ts, val] : samples_in_window)
            vals.push_back(static_cast<Float64>(val));

        const Float64 median = quantile(0.5, vals);

        VectorWithMemoryTracking<Float64> abs_devs;
        abs_devs.reserve(vals.size());
        for (Float64 v : vals)
            abs_devs.push_back(std::abs(v - median));

        const Float64 mad = quantile(0.5, abs_devs);

        result = static_cast<ValueType>(mad);
        null = 0;
    }
};

template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeMapBucket;

template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeSortedUniqueBucket;

template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeSingleSampleBucket;

template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeMapBucketPolicy
{
    static constexpr OverTimeBucketKind bucket_kind = OverTimeBucketKind::Map;
    using Bucket = TimeseriesOverTimeMapBucket<TimestampTypeT, ValueTypeT>;
};

template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeSortedUniqueBucketPolicy
{
    static constexpr OverTimeBucketKind bucket_kind = OverTimeBucketKind::SortedUnique;
    using Bucket = TimeseriesOverTimeSortedUniqueBucket<TimestampTypeT, ValueTypeT>;
};

template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeSingleSampleBucketPolicy
{
    static constexpr OverTimeBucketKind bucket_kind = OverTimeBucketKind::SingleSample;
    using Bucket = TimeseriesOverTimeSingleSampleBucket<TimestampTypeT, ValueTypeT>;
};

template <
    bool has_array_arguments,
    typename TimestampTypeT,
    typename IntervalTypeT,
    typename ValueTypeT,
    typename OperationT,
    template <typename, typename> class BucketPolicyT>
struct AggregateFunctionTimeseriesOverTimeTraits
{
    static constexpr bool array_arguments = has_array_arguments;

    using TimestampType = TimestampTypeT;
    using IntervalType = IntervalTypeT;
    using ValueType = ValueTypeT;
    using Operation = OperationT;
    using BucketPolicy = BucketPolicyT<TimestampType, ValueType>;
    using Bucket = typename BucketPolicy::Bucket;

    static constexpr OverTimeBucketKind bucket_kind = BucketPolicy::bucket_kind;

    static String getName() { return Operation::getName(); }
};

/// Per-bucket multiset with deduplication rule: duplicate timestamps merge by max(value). Used by aggregates that still
/// require all timestamps in-window (quantile, sum, avg, …).
template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeMapBucket
{
    using TimestampType = TimestampTypeT;
    using ValueType = ValueTypeT;

    absl::flat_hash_map<TimestampType, ValueType> samples;

    void add(TimestampType timestamp, ValueType value)
    {
        auto it = samples.find(timestamp);
        if (it != samples.end())
            it->second = std::max(it->second, value);
        else
            samples[timestamp] = value;
    }

    void merge(const TimeseriesOverTimeMapBucket & other)
    {
        samples.reserve(samples.size() + other.samples.size());
        for (const auto & [timestamp, value] : other.samples)
            add(timestamp, value);
    }
};

/// Same logical state as TimeseriesOverTimeMapBucket but stored as a sorted unique sequence of timestamps. Avoids hash
/// table overhead and allows pushing into the sliding window without an extra sort per bucket.
template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeSortedUniqueBucket
{
    using TimestampType = TimestampTypeT;
    using ValueType = ValueTypeT;

    VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> samples;

    void add(TimestampType timestamp, ValueType value)
    {
        auto it = std::lower_bound(
            samples.begin(), samples.end(), timestamp, [](const std::pair<TimestampType, ValueType> & a, const TimestampType & ts) { return a.first < ts; });

        if (it != samples.end() && it->first == timestamp)
            it->second = std::max(it->second, value);
        else
            samples.insert(it, {timestamp, value});
    }

    void merge(const TimeseriesOverTimeSortedUniqueBucket & other)
    {
        if (other.samples.empty())
            return;
        if (samples.empty())
        {
            samples = other.samples;
            return;
        }

        VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> merged;
        merged.reserve(samples.size() + other.samples.size());

        auto a = samples.begin();
        auto b = other.samples.begin();
        const auto a_end = samples.end();
        const auto b_end = other.samples.end();

        while (a != a_end && b != b_end)
        {
            if (a->first < b->first)
            {
                merged.push_back(*a);
                ++a;
            }
            else if (b->first < a->first)
            {
                merged.push_back(*b);
                ++b;
            }
            else
            {
                merged.push_back({a->first, std::max(a->second, b->second)});
                ++a;
                ++b;
            }
        }
        while (a != a_end)
        {
            merged.push_back(*a);
            ++a;
        }
        while (b != b_end)
        {
            merged.push_back(*b);
            ++b;
        }
        samples.swap(merged);
    }
};

/// Stores only the sample with the maximum timestamp per bucket. Sufficient when the operation only
/// needs to know whether any sample is present in the window (present/absent), or needs the latest
/// sample's timestamp or value (ts_of_last). When max_ts expires from the sliding window, all other
/// samples in the bucket (ts ≤ max_ts) are also guaranteed to have expired.
template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeSingleSampleBucket
{
    using TimestampType = TimestampTypeT;
    using ValueType = ValueTypeT;

    TimestampType max_ts{};
    ValueType value_at_max_ts{};
    bool has_sample{false};

    void add(TimestampType timestamp, ValueType value)
    {
        if (!has_sample || timestamp > max_ts)
        {
            max_ts = timestamp;
            value_at_max_ts = value;
            has_sample = true;
        }
        else if (timestamp == max_ts)
        {
            value_at_max_ts = std::max(value_at_max_ts, value);
        }
    }

    void merge(const TimeseriesOverTimeSingleSampleBucket & other)
    {
        if (other.has_sample)
            add(other.max_ts, other.value_at_max_ts);
    }
};

/// Template aliases matching the 5-parameter template template signature required by
/// createAggregateFunctionTimeseries. The 5th bool parameter is unused for _over_time functions.
template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesAvgOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesAvgOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeMapBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesMinOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesMinOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSortedUniqueBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesMaxOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesMaxOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSortedUniqueBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesSumOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesSumOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeMapBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesCountOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesCountOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeMapBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesStddevPopOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesStddevPopOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeMapBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesStdvarPopOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesStdvarPopOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeMapBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesPresentOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesPresentOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSingleSampleBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesAbsentOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesAbsentOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSingleSampleBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesFirstOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesFirstOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSortedUniqueBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfLastOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesTsOfLastOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSingleSampleBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfFirstOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesTsOfFirstOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSortedUniqueBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfMinOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesTsOfMinOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSortedUniqueBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfMaxOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesTsOfMaxOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSortedUniqueBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesQuantileOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesQuantileOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeMapBucketPolicy>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesMadOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments,
    TimestampType,
    IntervalType,
    ValueType,
    AggregateFunctionTimeseriesMadOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeMapBucketPolicy>;


template <typename Traits>
class AggregateFunctionTimeseriesOverTime final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTime<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

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
        if constexpr (Traits::bucket_kind == OverTimeBucketKind::SingleSample)
        {
            writeBinaryLittleEndian(bucket.has_sample ? size_t(1) : size_t(0), buf);
            if (bucket.has_sample)
            {
                writeBinaryLittleEndian(bucket.max_ts, buf);
                writeBinaryLittleEndian(bucket.value_at_max_ts, buf);
            }
        }
        else
        {
            writeBinaryLittleEndian(bucket.samples.size(), buf);
            for (const auto & sample : bucket.samples)
            {
                writeBinaryLittleEndian(sample.first, buf);
                writeBinaryLittleEndian(sample.second, buf);
            }
        }
    }

    void deserializeBucket(Bucket & bucket, ReadBuffer & buf, const size_t bucket_index) const
    {
        size_t sample_count;
        readBinaryLittleEndian(sample_count, buf);
        if constexpr (Traits::bucket_kind == OverTimeBucketKind::Map || Traits::bucket_kind == OverTimeBucketKind::SortedUnique)
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
        Traits::Operation::fillResultValue(samples_in_window, result, null, extra_param, Base::timestamp_scale_multiplier);
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
                if constexpr (Traits::bucket_kind == OverTimeBucketKind::Map)
                {
                    timestamps_buffer.clear();
                    for (const auto & [timestamp, value] : bucket_it->second.samples)
                        timestamps_buffer.emplace_back(timestamp, value);
                    std::sort(timestamps_buffer.begin(), timestamps_buffer.end());

                    for (const auto & [timestamp, value] : timestamps_buffer)
                        samples_in_window.push_back({timestamp, value});
                }
                else if constexpr (Traits::bucket_kind == OverTimeBucketKind::SortedUnique)
                {
                    for (const auto & sample : bucket_it->second.samples)
                        samples_in_window.push_back(sample);
                }
                else /// SingleSample
                {
                    if (bucket_it->second.has_sample)
                        samples_in_window.push_back({bucket_it->second.max_ts, bucket_it->second.value_at_max_ts});
                }
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
