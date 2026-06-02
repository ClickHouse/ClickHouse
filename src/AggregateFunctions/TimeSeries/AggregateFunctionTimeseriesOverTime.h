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


namespace DB
{

/// Mirrors Prometheus `promql/quantile.go` func quantile(q float64, values vectorByValueHeap) float64
/// https://github.com/prometheus/prometheus/blob/da1f89e7360a19c5de2b0df4b43411ac706a76a9/promql/quantile.go
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

/// Per-bucket samples kept in an append-only vector. Sort + dedup-by-max(value) (per timestamp)
/// is performed lazily on the first read, then memoized via `sorted`. Compared with the previous
/// `absl::flat_hash_map`-backed bucket this trades the hash table's per-sample lookup and slot
/// overhead for a single in-place sort per state, and shrinks per-sample memory roughly 2x.
/// Logical semantics are preserved: when two samples within a bucket share a timestamp, the
/// larger value wins.
///
/// Used by aggregates that need every sample in the active window (avg, sum, count, min, max,
/// first, stddev, stdvar, quantile, mad, ts_of_first, ts_of_min, ts_of_max).
template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeVectorBucket
{
    using TimestampType = TimestampTypeT;
    using ValueType = ValueTypeT;

    /// Logical state of the bucket is the set of (timestamp, max-merged value) samples. Sorting
    /// and deduplicating only canonicalizes the internal order, so the storage is `mutable` and
    /// the lazy work can run from `const` observers (`serialize`, `appendActiveSamplesToWindow`).
    mutable VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> samples;
    mutable bool sorted = true;

    void add(TimestampType timestamp, ValueType value)
    {
        samples.emplace_back(timestamp, value);
        sorted = false;
    }

    void merge(const TimeseriesOverTimeVectorBucket & other)
    {
        if (other.samples.empty())
            return;
        samples.reserve(samples.size() + other.samples.size());
        for (const auto & sample : other.samples)
            samples.push_back(sample);
        sorted = false;
    }

    /// Sort by timestamp ascending and collapse runs of equal timestamps by `max(value)`.
    /// Idempotent; the cached flag short-circuits repeated calls.
    void sortAndDedup() const
    {
        if (sorted)
            return;

        std::sort(samples.begin(), samples.end(),
            [](const auto & a, const auto & b) { return a.first < b.first; });

        size_t write = 0;
        for (size_t read = 0; read < samples.size(); ++read)
        {
            if (write > 0 && samples[write - 1].first == samples[read].first)
                samples[write - 1].second = std::max(samples[write - 1].second, samples[read].second);
            else
            {
                if (write != read)
                    samples[write] = samples[read];
                ++write;
            }
        }
        samples.resize(write);
        sorted = true;
    }

    /// Append samples in the Prometheus half-open lookback `(current_timestamp - window, current_timestamp]`.
    /// A sample is active iff `ts + window > current_timestamp` (equivalently `ts > current_timestamp - window`,
    /// but we avoid the subtraction so unsigned `TimestampType` never underflows).
    template <typename WindowType>
    void appendActiveSamplesToWindow(
        DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window,
        TimestampType current_timestamp,
        WindowType window) const
    {
        sortAndDedup();
        for (const auto & sample : samples)
        {
            if (sample.first + window > current_timestamp)
                samples_in_window.push_back(sample);
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        sortAndDedup();
        writeBinaryLittleEndian(samples.size(), buf);
        for (const auto & [timestamp, value] : samples)
        {
            writeBinaryLittleEndian(timestamp, buf);
            writeBinaryLittleEndian(value, buf);
        }
    }

    template <typename CheckTimestampInRange>
    void deserialize(ReadBuffer & buf, CheckTimestampInRange && check_timestamp_in_range)
    {
        size_t sample_count = 0;
        readBinaryLittleEndian(sample_count, buf);

        samples.clear();
        samples.reserve(sample_count);

        for (size_t s = 0; s < sample_count; ++s)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            check_timestamp_in_range(timestamp);

            ValueType value;
            readBinaryLittleEndian(value, buf);

            samples.emplace_back(timestamp, value);
        }
        /// Payloads written by older versions used an unspecified hash-map iteration order, so
        /// the deserialized vector may need canonicalizing before the first read.
        sorted = false;
    }
};

/// Stores only the sample with the maximum timestamp per bucket. Sufficient when the operation only
/// needs to know whether any sample is present in the window (present/absent) or needs the latest
/// sample's timestamp or value (ts_of_last). When `max_ts` expires from the sliding window, all other
/// samples in the bucket (which have ts ≤ max_ts) are guaranteed to have expired too.
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

    template <typename WindowType>
    void appendActiveSamplesToWindow(
        DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> & samples_in_window,
        TimestampType current_timestamp,
        WindowType window) const
    {
        if (has_sample && max_ts + window > current_timestamp)
            samples_in_window.push_back({max_ts, value_at_max_ts});
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(has_sample ? size_t(1) : size_t(0), buf);
        if (has_sample)
        {
            writeBinaryLittleEndian(max_ts, buf);
            writeBinaryLittleEndian(value_at_max_ts, buf);
        }
    }

    template <typename CheckTimestampInRange>
    void deserialize(ReadBuffer & buf, CheckTimestampInRange && check_timestamp_in_range)
    {
        has_sample = false;
        max_ts = {};
        value_at_max_ts = {};

        size_t sample_count = 0;
        readBinaryLittleEndian(sample_count, buf);

        for (size_t s = 0; s < sample_count; ++s)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            check_timestamp_in_range(timestamp);

            ValueType value;
            readBinaryLittleEndian(value, buf);

            add(timestamp, value);
        }
    }
};

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

        auto best = samples_in_window.front();
        for (const auto & sample : samples_in_window)
        {
            /// Match VictoriaMetrics `rollupTmin`: last timestamp for the minimum value.
            if (sample.second <= best.second)
                best = sample;
        }

        Float64 ts_sec = static_cast<Float64>(best.first) / static_cast<Float64>(timestamp_scale_multiplier);
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

        auto best = samples_in_window.front();
        for (const auto & sample : samples_in_window)
        {
            /// Match VictoriaMetrics `rollupTmax`: last timestamp for the maximum value.
            if (sample.second >= best.second)
                best = sample;
        }

        Float64 ts_sec = static_cast<Float64>(best.first) / static_cast<Float64>(timestamp_scale_multiplier);
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
        /// https://github.com/prometheus/prometheus/blob/da1f89e7360a19c5de2b0df4b43411ac706a76a9/promql/quantile.go
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

template <
    bool has_array_arguments,
    typename TimestampTypeT,
    typename IntervalTypeT,
    typename ValueTypeT,
    typename OperationT,
    template <typename, typename> class BucketT>
struct AggregateFunctionTimeseriesOverTimeTraits
{
    static constexpr bool array_arguments = has_array_arguments;

    using TimestampType = TimestampTypeT;
    using IntervalType = IntervalTypeT;
    using ValueType = ValueTypeT;
    using Operation = OperationT;
    using Bucket = BucketT<TimestampType, ValueType>;

    static String getName() { return Operation::getName(); }
};

/// Template aliases matching the 5-parameter template template signature required by
/// `createAggregateFunctionTimeseries`. The 5th bool parameter is unused for _over_time functions.
template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesAvgOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesAvgOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesMinOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesMinOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesMaxOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesMaxOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesSumOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesSumOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesCountOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesCountOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesStddevPopOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesStddevPopOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesStdvarPopOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesStdvarPopOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesPresentOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesPresentOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSingleSampleBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesAbsentOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesAbsentOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSingleSampleBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesFirstOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesFirstOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfLastOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesTsOfLastOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSingleSampleBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfFirstOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesTsOfFirstOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfMinOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesTsOfMinOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfMaxOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesTsOfMaxOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesQuantileOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesQuantileOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesMadOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesMadOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;


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
        bucket.serialize(buf);
    }

    void deserializeBucket(Bucket & bucket, ReadBuffer & buf, const size_t bucket_index) const
    {
        bucket.deserialize(buf, [&](TimestampType timestamp)
        {
            Base::checkTimestampInRange(timestamp, bucket_index);
        });
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

        for (UInt32 i = 0; i < Base::bucket_count; ++i)
        {
            const TimestampType current_timestamp = Base::start_timestamp + i * Base::step;

            /// Pop before ingesting the current bucket so nothing outside the window is ever observed by `fillResultValue`.
            while (!samples_in_window.empty() && samples_in_window.front().first + Base::window <= current_timestamp)
                samples_in_window.pop_front();

            auto bucket_it = buckets.find(i);
            if (bucket_it != buckets.end())
                bucket_it->second.appendActiveSamplesToWindow(samples_in_window, current_timestamp, Base::window);

            Traits::Operation::fillResultValue(samples_in_window, values[i], nulls[i], extra_param, Base::timestamp_scale_multiplier);
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 1;

private:
    Float64 extra_param;
};

}
