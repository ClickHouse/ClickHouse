#pragma once

/// Ports of `histogramRate`/`extrapolatedRate` (`rate`/`increase`/`delta`) and the histogram branch of `instantValue` (`irate`/`idelta`)
/// from pinned upstream tmp/upstream_slice4_prom_functions.go: start timestamps omitted, annotations dropped, Float64-second arithmetic.

#include <algorithm>
#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include <base/extended_types.h>
#include <Functions/TimeSeries/TimeSeriesHistogramKernel.h>


namespace DB
{

/// The port of `histogramRate`: the reset-aware difference of the window's last and first samples (`points` ascending,
/// `is_counter` mirrors isCounter); returns nullopt for fewer than two samples or a mix of exponential and custom buckets.
template <typename TimestampType>
std::optional<TimeSeriesFloatHistogram> timeSeriesHistogramRate(
    const std::vector<std::pair<TimestampType, TimeSeriesFloatHistogram>> & points, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    bool is_counter)
{
    const size_t num_points = points.size();
    if (num_points < 2)
        return std::nullopt;

    TimeSeriesFloatHistogram prev = points[0].second;
    const TimeSeriesFloatHistogram & last = points[num_points - 1].second;

    /// Null out the 1st sample if there is a counter reset between the 1st and 2nd: any
    /// incompatibility in the bucket layout of the 1st sample is ignored because it is not looked at.
    if (is_counter && points[1].second.detectReset(prev))
    {
        prev = TimeSeriesFloatHistogram{};
        prev.schema = points[1].second.schema;
        prev.custom_values = points[1].second.custom_values;
    }

    const bool using_custom_buckets = prev.usesCustomBuckets();
    if (last.usesCustomBuckets() != using_custom_buckets)
        return std::nullopt;

    /// The smallest relevant schema (upstream's gauge-hint check here only produces dropped annotations); the middle-sample
    /// scan is counter-only upstream, so `delta` keeps min(first, last) and never drops over a custom-bucket middle sample.
    Int32 min_schema = std::min(last.schema, prev.schema);
    if (is_counter)
    {
        for (size_t i = 1; i + 1 < num_points; ++i)
        {
            if (points[i].second.usesCustomBuckets() != using_custom_buckets)
                return std::nullopt;
            min_schema = std::min(min_schema, points[i].second.schema);
        }
    }

    TimeSeriesFloatHistogram h = last.copyToSchema(min_schema);
    /// This subtraction may deliberately include conflicting counter resets; those are treated explicitly here,
    /// so the collision/reconciliation outcomes of `sub`/`add` are ignored (upstream warns via annotations).
    h.sub(prev);

    if (is_counter)
    {
        /// Second iteration to deal with counter resets (no start timestamps: the upstream
        /// isStartTimestampReset branch is omitted).
        for (size_t i = 1; i < num_points; ++i)
        {
            const TimeSeriesFloatHistogram & curr = points[i].second;
            if (curr.detectReset(prev))
                h.add(prev);
            prev = curr;
        }
    }

    h.counter_reset_hint = TimeSeriesHistogramCounterResetHint::GaugeType;
    h.compact(0);
    return h;
}

/// The port of `extrapolatedRate` (without start timestamps): the `timeSeriesHistogramRate` difference extrapolated
/// to the window's boundaries; `grid_timestamp`/`window` are in timestamp units, `timestamp_scale` converts to seconds.
template <typename TimestampType, typename IntervalType>
std::optional<TimeSeriesFloatHistogram> timeSeriesHistogramExtrapolatedRate(
    const std::vector<std::pair<TimestampType, TimeSeriesFloatHistogram>> & points, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    TimestampType grid_timestamp, IntervalType window, Float64 timestamp_scale,
    bool is_rate, bool is_counter)
{
    const size_t num_points = points.size();
    /// A single sample yields nothing without a start timestamp (upstream's
    /// numSamplesMinusOne == 0 branch).
    if (num_points < 2)
        return std::nullopt;

    auto result = timeSeriesHistogramRate(points, is_counter);
    if (!result)
        return std::nullopt;

    const TimestampType first_timestamp = points.front().first;
    const TimestampType last_timestamp = points.back().first;

    /// Durations between the first/last samples and the range boundary, in seconds; the integer subtraction is done
    /// in Int128 (like `AggregateFunctionTimeseriesExtrapolatedValue`), so neither overflow nor Float64 precision loss can occur.
    Float64 duration_to_start = static_cast<Float64>(
        static_cast<Int128>(static_cast<Int64>(first_timestamp))
        - static_cast<Int128>(static_cast<Int64>(grid_timestamp))
        + static_cast<Int128>(static_cast<Int64>(window))) / timestamp_scale;
    Float64 duration_to_end = static_cast<Float64>(
        static_cast<Int128>(static_cast<Int64>(grid_timestamp))
        - static_cast<Int128>(static_cast<Int64>(last_timestamp))) / timestamp_scale;
    const Float64 sampled_interval = static_cast<Float64>(
        static_cast<Int128>(static_cast<Int64>(last_timestamp))
        - static_cast<Int128>(static_cast<Int64>(first_timestamp))) / timestamp_scale;

    const Float64 average_duration_between_samples = sampled_interval / static_cast<Float64>(num_points - 1);
    const Float64 extrapolation_threshold = average_duration_between_samples * 1.1;

    /// Extrapolate all the way to a boundary when samples are within 10% over the average sample distance of it,
    /// otherwise only by half the average distance (the guess for where the series actually starts or ends).
    if (duration_to_start >= extrapolation_threshold)
        duration_to_start = average_duration_between_samples / 2;

    if (is_counter)
    {
        /// Counters cannot be negative: extrapolate the counter's zero point and, if it is closer than
        /// duration_to_start, take it as the start of the series, avoiding negative counter values.
        Float64 duration_to_zero = duration_to_start;
        if (result->count > 0 && points.front().second.count >= 0)
            duration_to_zero = sampled_interval * (points.front().second.count / result->count);
        duration_to_start = std::min(duration_to_start, duration_to_zero);
    }

    if (duration_to_end >= extrapolation_threshold)
        duration_to_end = average_duration_between_samples / 2;

    Float64 factor = 1.0;
    if (sampled_interval != 0)
        factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;
    if (is_rate)
        factor /= static_cast<Float64>(window) / timestamp_scale;

    result->mul(factor);
    return result;
}

/// The port of the histogram branch of `instantValue` (`irate`/`idelta`): the difference of the two most recent samples,
/// no extrapolation; `irate` is reset-aware and divides by the interval in seconds, `idelta` always subtracts.
template <typename TimestampType>
std::optional<TimeSeriesFloatHistogram> timeSeriesHistogramInstantValue(
    const TimeSeriesFloatHistogram & previous, TimestampType previous_timestamp,
    const TimeSeriesFloatHistogram & current, TimestampType current_timestamp,
    Float64 timestamp_scale, bool is_rate)
{
    /// The equality is overflow-insensitive (the wrapping subtraction is a bijection).
    if (current_timestamp == previous_timestamp)
        return std::nullopt;
    const Float64 interval_seconds = static_cast<Float64>(
        static_cast<Int128>(static_cast<Int64>(current_timestamp))
        - static_cast<Int128>(static_cast<Int64>(previous_timestamp))) / timestamp_scale;

    TimeSeriesFloatHistogram result = current;
    /// The outcomes of `sub` are ignored: conflicting resets are treated explicitly (upstream warns via annotations);
    /// a mixed exponential/custom pair returns nullopt like upstream's drop instead of the kernel's exception.
    if (current.usesCustomBuckets() != previous.usesCustomBuckets())
        return std::nullopt;
    if (!is_rate || !current.detectReset(previous))
        result.sub(previous);
    result.counter_reset_hint = TimeSeriesHistogramCounterResetHint::GaugeType;
    result.compact(0);

    if (is_rate)
        result.div(interval_seconds);
    return result;
}

}
