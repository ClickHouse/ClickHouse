#pragma once

#include <algorithm>
#include <cmath>
#include <utility>

#include <base/types.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesOverTimeBuckets.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesOverTimeHelpers.h>


namespace DB
{

/// Per-aggregate `Operation` structs for the `_over_time` family.
///
/// Two flavors live here:
///
///   - **Baseline operations** (`AggregateFunctionTimeseries*OverTimeOperation`).
///     Driven by `AggregateFunctionTimeseriesOverTime`, they receive every sample in the
///     active window as a `DequeWithMemoryTracking<(ts, value)>` and scan it on every grid.
///
///   - **`_stats` aligned operations** (`*OverTimeStatsAlignedOperation`).
///     Driven by the `AggregateFunctionTimeseriesOverTimeStatsAligned*` classes
///     (running-stats / mono-deque / two-pointer). They never see raw samples — instead
///     they read a running `Accumulator` or a single `Bucket` selected by the driver.
///
/// Operations are tiny `static`-only structs so they can be plugged into `Traits` without
/// runtime dispatch.


/// ---------------------------------------------------------------------------
/// Baseline `_over_time` operations (driven by `AggregateFunctionTimeseriesOverTime`).
/// ---------------------------------------------------------------------------

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


/// ---------------------------------------------------------------------------
/// Accumulator-driven `_stats` Operations (`O(grid)`).
/// ---------------------------------------------------------------------------
/// Read a running `Accumulator` maintained by `AggregateFunctionTimeseriesOverTimeStatsAligned`,
/// which slides `addBucket / subBucket` across the aligned window.

/// `count_over_time` on aligned windows: read `Accumulator::count` populated by the driver
/// from each bucket's running counter (`TimeseriesOverTimeAddableStatsBucket::count`).
template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesCountOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimeAddableStatsBucket<TimestampType, ValueType>;
    using Accumulator = AggregateFunctionTimeseriesAddableStatsAccumulator<TimestampType, ValueType>;

    static String getName() { return "timeSeriesCountOverTimeToGrid_stats"; }

    static void fillResultValue(const Accumulator & acc, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (acc.count == 0)
        {
            result = 0;
            null = 1;
            return;
        }
        result = static_cast<ValueType>(acc.count);
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesSumOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimeAddableStatsBucket<TimestampType, ValueType>;
    using Accumulator = AggregateFunctionTimeseriesAddableStatsAccumulator<TimestampType, ValueType>;

    static String getName() { return "timeSeriesSumOverTimeToGrid_stats"; }

    static void fillResultValue(const Accumulator & acc, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (acc.count == 0)
        {
            result = 0;
            null = 1;
            return;
        }
        result = static_cast<ValueType>(acc.sum);
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesAvgOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimeAddableStatsBucket<TimestampType, ValueType>;
    using Accumulator = AggregateFunctionTimeseriesAddableStatsAccumulator<TimestampType, ValueType>;

    static String getName() { return "timeSeriesAvgOverTimeToGrid_stats"; }

    static void fillResultValue(const Accumulator & acc, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (acc.count == 0)
        {
            result = 0;
            null = 1;
            return;
        }
        result = static_cast<ValueType>(acc.sum / static_cast<Float64>(acc.count));
        null = 0;
    }
};

/// Population variance: `E[x²] - (E[x])²`. Matches the baseline `stdvar_over_time`
/// (see `AggregateFunctionTimeseriesStdvarPopOverTimeOperation`).
template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesStdvarPopOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimeAddableStatsBucket<TimestampType, ValueType>;
    using Accumulator = AggregateFunctionTimeseriesAddableStatsAccumulator<TimestampType, ValueType>;

    static String getName() { return "timeSeriesStdvarOverTimeToGrid_stats"; }

    static void fillResultValue(const Accumulator & acc, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (acc.count == 0)
        {
            result = 0;
            null = 1;
            return;
        }
        const Float64 n = static_cast<Float64>(acc.count);
        const Float64 mean = acc.sum / n;
        const Float64 mean_sq = acc.sum_sq / n;
        const Float64 variance = std::max(Float64{0}, mean_sq - mean * mean);
        result = static_cast<ValueType>(variance);
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesStddevPopOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimeAddableStatsBucket<TimestampType, ValueType>;
    using Accumulator = AggregateFunctionTimeseriesAddableStatsAccumulator<TimestampType, ValueType>;

    static String getName() { return "timeSeriesStddevOverTimeToGrid_stats"; }

    static void fillResultValue(const Accumulator & acc, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (acc.count == 0)
        {
            result = 0;
            null = 1;
            return;
        }
        const Float64 n = static_cast<Float64>(acc.count);
        const Float64 mean = acc.sum / n;
        const Float64 mean_sq = acc.sum_sq / n;
        const Float64 variance = std::max(Float64{0}, mean_sq - mean * mean);
        result = static_cast<ValueType>(std::sqrt(variance));
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesPresentOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimePresenceStatsBucket<TimestampType, ValueType>;
    using Accumulator = AggregateFunctionTimeseriesPresenceStatsAccumulator<TimestampType, ValueType>;

    static String getName() { return "timeSeriesPresentOverTimeToGrid_stats"; }

    static void fillResultValue(const Accumulator & acc, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (acc.active_count == 0)
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
struct AggregateFunctionTimeseriesAbsentOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimePresenceStatsBucket<TimestampType, ValueType>;
    using Accumulator = AggregateFunctionTimeseriesPresenceStatsAccumulator<TimestampType, ValueType>;

    static String getName() { return "timeSeriesAbsentOverTimeToGrid_stats"; }

    static void fillResultValue(const Accumulator & acc, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        if (acc.active_count == 0)
        {
            result = static_cast<ValueType>(1);
            null = 0;
            return;
        }
        result = 0;
        null = 1;
    }
};


/// ---------------------------------------------------------------------------
/// Mono-deque-driven Operations (`min / max / ts_of_min / ts_of_max`).
/// ---------------------------------------------------------------------------
/// Contract:
///   - `Bucket` exposes `has_sample` and a per-bucket extreme value (e.g. `min_val` /
///     `max_val`, plus `ts_at_min` / `ts_at_max` for the `ts_of_min` / `ts_of_max` family).
///   - `shouldEvictBack(back, cur)` is used by `runMonoDequeAlignedDriver` to decide
///     whether the rear of the monotonic deque should be popped when `cur` arrives.
///     Returning `true` on ties (e.g. `back.max_val <= cur.max_val`) makes the **later**
///     bucket win, matching VictoriaMetrics `rollupTmax / rollupTmin` semantics.
///   - `fillResultValue(front_bucket, result, null, extra, scale)` reads the deque's front
///     bucket (= global extreme over the active window).

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesMinOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimeMinMaxStatsBucket<TimestampType, ValueType>;

    static String getName() { return "timeSeriesMinOverTimeToGrid_stats"; }

    static bool shouldEvictBack(const Bucket & back, const Bucket & cur)
    {
        return back.min_val >= cur.min_val;
    }

    static void fillResultValue(const Bucket & front, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        result = front.min_val;
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesMaxOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimeMinMaxStatsBucket<TimestampType, ValueType>;

    static String getName() { return "timeSeriesMaxOverTimeToGrid_stats"; }

    static bool shouldEvictBack(const Bucket & back, const Bucket & cur)
    {
        return back.max_val <= cur.max_val;
    }

    static void fillResultValue(const Bucket & front, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        result = front.max_val;
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesTsOfMinOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimeTsAtExtremeStatsBucket<TimestampType, ValueType>;

    static String getName() { return "timeSeriesTsOfMinOverTimeToGrid_stats"; }

    static bool shouldEvictBack(const Bucket & back, const Bucket & cur)
    {
        /// `<=` on tie keeps the LATER bucket — VM `rollupTmin` returns the LAST ts for the min value.
        return back.min_val >= cur.min_val;
    }

    static void fillResultValue(const Bucket & front, ValueType & result, UInt8 & null, Float64, TimestampType timestamp_scale_multiplier)
    {
        const Float64 ts_sec = static_cast<Float64>(front.ts_at_min) / static_cast<Float64>(timestamp_scale_multiplier);
        result = static_cast<ValueType>(ts_sec);
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesTsOfMaxOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimeTsAtExtremeStatsBucket<TimestampType, ValueType>;

    static String getName() { return "timeSeriesTsOfMaxOverTimeToGrid_stats"; }

    static bool shouldEvictBack(const Bucket & back, const Bucket & cur)
    {
        return back.max_val <= cur.max_val;
    }

    static void fillResultValue(const Bucket & front, ValueType & result, UInt8 & null, Float64, TimestampType timestamp_scale_multiplier)
    {
        const Float64 ts_sec = static_cast<Float64>(front.ts_at_max) / static_cast<Float64>(timestamp_scale_multiplier);
        result = static_cast<ValueType>(ts_sec);
        null = 0;
    }
};


/// ---------------------------------------------------------------------------
/// Two-pointer-driven Operations (`first / last / ts_of_first / ts_of_last`).
/// ---------------------------------------------------------------------------
/// Contract:
///   - `Bucket` exposes `has_sample` plus the extreme-sample data (min_ts/value_at_min_ts
///     for first; max_ts/value_at_max_ts for last via SingleSampleBucket).
///   - `fillResultValue(bucket, result, null, extra, scale)` reads the single active
///     bucket the driver pinpointed (leftmost-in-window for first, rightmost for last).

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesFirstOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimeFirstSampleBucket<TimestampType, ValueType>;

    static String getName() { return "timeSeriesFirstOverTimeToGrid_stats"; }

    static void fillResultValue(const Bucket & bucket, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        result = bucket.value_at_min_ts;
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesTsOfFirstOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimeFirstSampleBucket<TimestampType, ValueType>;

    static String getName() { return "timeSeriesTsOfFirstOverTimeToGrid_stats"; }

    static void fillResultValue(const Bucket & bucket, ValueType & result, UInt8 & null, Float64, TimestampType timestamp_scale_multiplier)
    {
        const Float64 ts_sec = static_cast<Float64>(bucket.min_ts) / static_cast<Float64>(timestamp_scale_multiplier);
        result = static_cast<ValueType>(ts_sec);
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesLastOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimeSingleSampleBucket<TimestampType, ValueType>;

    static String getName() { return "timeSeriesLastOverTimeToGrid_stats"; }

    static void fillResultValue(const Bucket & bucket, ValueType & result, UInt8 & null, Float64, TimestampType)
    {
        result = bucket.value_at_max_ts;
        null = 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesTsOfLastOverTimeStatsAlignedOperation
{
    using Bucket = TimeseriesOverTimeSingleSampleBucket<TimestampType, ValueType>;

    static String getName() { return "timeSeriesTsOfLastOverTimeToGrid_stats"; }

    static void fillResultValue(const Bucket & bucket, ValueType & result, UInt8 & null, Float64, TimestampType timestamp_scale_multiplier)
    {
        const Float64 ts_sec = static_cast<Float64>(bucket.max_ts) / static_cast<Float64>(timestamp_scale_multiplier);
        result = static_cast<ValueType>(ts_sec);
        null = 0;
    }
};

}
