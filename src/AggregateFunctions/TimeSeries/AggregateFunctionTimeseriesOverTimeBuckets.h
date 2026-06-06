#pragma once

#include <algorithm>
#include <cstddef>
#include <utility>

#include <base/types.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/DequeWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

/// Per-bucket sample-storage types used by the `_over_time` aggregates.
///
/// Each bucket implements `add(ts, value)`, `merge(other)`, `serialize`, `deserialize`,
/// and either `appendActiveSamplesToWindow(...)` (baseline path) or stats-specific
/// accessors (`flushDedup`, `has_sample`, running counters, …) consumed by the aligned
/// drivers in `AggregateFunctionTimeseriesOverTimeStatsAligned.h`. Two helper
/// `Accumulator` structs live alongside the buckets they read from.


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

    VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> samples;
    bool sorted = true;

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
    void sortAndDedup()
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
        WindowType window)
    {
        sortAndDedup();
        for (const auto & sample : samples)
        {
            if (sample.first + window > current_timestamp)
                samples_in_window.push_back(sample);
        }
    }

    void serialize(WriteBuffer & buf)
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

/// Bucket holding only running `(count, sum, sum_sq)` per bucket, used by additive aggregates
/// (`count / sum / avg / stddev / stdvar _over_time`). `add` updates the three counters in O(1)
/// with no per-sample allocation; the aligned driver in
/// `AggregateFunctionTimeseriesOverTimeStatsAligned` then slides them across the window via
/// `addBucket / subBucket` in `O(grid)` total.
///
/// **Same-`ts` policy** — this bucket does NOT collapse duplicate timestamps by `max(value)`
/// before counting; every `add` call contributes once. The ClickHouse TimeSeries ingest path
/// (`prometheus_remote_write`) and Prometheus / VictoriaMetrics upstreams guarantee strictly
/// increasing per-series timestamps, so duplicates do not arise in practice. The baseline
/// vector-bucket aggregate (`TimeseriesOverTimeVectorBucket`) still performs PromQL-strict
/// `max`-merge per ts; callers that need that semantic should set
/// `prometheus_query_use_stats_bucket = 0`.
///
/// Bucket 0 must have the same width as every other bucket (samples in `(start - step, start]`
/// only); enforced by `Traits::strict_pre_window_filter == true`. With the loose filter,
/// bucket 0 would also accept pre-start samples that the running-stats driver cannot
/// sub-filter — silently producing the wrong result for grid 0.
template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeAddableStatsBucket
{
    using TimestampType = TimestampTypeT;
    using ValueType = ValueTypeT;

    UInt32 count = 0;
    Float64 sum = 0;
    Float64 sum_sq = 0;

    void add(TimestampType, ValueType value)
    {
        const Float64 v = static_cast<Float64>(value);
        ++count;
        sum += v;
        sum_sq += v * v;
    }

    void merge(const TimeseriesOverTimeAddableStatsBucket & other)
    {
        count += other.count;
        sum += other.sum;
        sum_sq += other.sum_sq;
    }

    /// Symmetric with `*StatsBucket::flushDedup` so the running-accumulator driver in
    /// `AggregateFunctionTimeseriesOverTimeStatsAligned` can call it uniformly. No-op here:
    /// the running stats are kept current eagerly in `add` / `merge`.
    void flushDedup() const {}

    void serialize(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(count, buf);
        writeBinaryLittleEndian(sum, buf);
        writeBinaryLittleEndian(sum_sq, buf);
    }

    template <typename CheckTimestampInRange>
    void deserialize(ReadBuffer & buf, CheckTimestampInRange && /*check_timestamp_in_range*/)
    {
        readBinaryLittleEndian(count, buf);
        readBinaryLittleEndian(sum, buf);
        readBinaryLittleEndian(sum_sq, buf);
    }
};

/// Running `(count, sum, sum_sq)` shared by all additive aggregates. The aligned driver
/// slides it across the window with O(1) per grid point via `addBucket / subBucket`.
template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesAddableStatsAccumulator
{
    using Bucket = TimeseriesOverTimeAddableStatsBucket<TimestampType, ValueType>;

    UInt32 count = 0;
    Float64 sum = 0;
    Float64 sum_sq = 0;

    void addBucket(const Bucket & b)
    {
        count += b.count;
        sum += b.sum;
        sum_sq += b.sum_sq;
    }

    void subBucket(const Bucket & b)
    {
        count -= b.count;
        sum -= b.sum;
        sum_sq -= b.sum_sq;
    }
};

/// Bucket for `present_over_time` / `absent_over_time`: a single bit recording whether the
/// bucket has any sample. The driver only needs to know whether *any* bucket in `[i-k+1, i]`
/// is non-empty, so it tracks a running count of active buckets and adds/subtracts `1` as
/// each bucket slides in / out of the window.
template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimePresenceStatsBucket
{
    using TimestampType = TimestampTypeT;
    using ValueType = ValueTypeT;

    bool has_sample = false;

    void add(TimestampType, ValueType)
    {
        has_sample = true;
    }

    void merge(const TimeseriesOverTimePresenceStatsBucket & other)
    {
        has_sample = has_sample || other.has_sample;
    }

    /// Symmetric API with `AddableStatsBucket` so the aligned driver can call it uniformly.
    void flushDedup() const {}

    void serialize(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(static_cast<UInt8>(has_sample ? 1 : 0), buf);
    }

    template <typename CheckTimestampInRange>
    void deserialize(ReadBuffer & buf, CheckTimestampInRange && /*check_timestamp_in_range*/)
    {
        UInt8 flag = 0;
        readBinaryLittleEndian(flag, buf);
        has_sample = flag != 0;
    }
};

template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesPresenceStatsAccumulator
{
    using Bucket = TimeseriesOverTimePresenceStatsBucket<TimestampType, ValueType>;

    UInt32 active_count = 0;

    void addBucket(const Bucket & b) { active_count += b.has_sample ? 1u : 0u; }
    void subBucket(const Bucket & b) { active_count -= b.has_sample ? 1u : 0u; }
};

/// Bucket for `min_over_time` / `max_over_time`: per-bucket extrema only. The mono-deque
/// driver in `AggregateFunctionTimeseriesOverTimeStatsAlignedMonoDeque` slides them across
/// the window in `O(grid)` amortized.
template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeMinMaxStatsBucket
{
    using TimestampType = TimestampTypeT;
    using ValueType = ValueTypeT;

    bool has_sample = false;
    ValueType min_val{};
    ValueType max_val{};

    void add(TimestampType, ValueType value)
    {
        if (!has_sample)
        {
            min_val = max_val = value;
            has_sample = true;
        }
        else
        {
            min_val = std::min(min_val, value);
            max_val = std::max(max_val, value);
        }
    }

    void merge(const TimeseriesOverTimeMinMaxStatsBucket & other)
    {
        if (!other.has_sample)
            return;
        if (!has_sample)
        {
            *this = other;
            return;
        }
        min_val = std::min(min_val, other.min_val);
        max_val = std::max(max_val, other.max_val);
    }

    void flushDedup() const {}

    void serialize(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(static_cast<UInt8>(has_sample ? 1 : 0), buf);
        writeBinaryLittleEndian(min_val, buf);
        writeBinaryLittleEndian(max_val, buf);
    }

    template <typename CheckTimestampInRange>
    void deserialize(ReadBuffer & buf, CheckTimestampInRange && /*check_timestamp_in_range*/)
    {
        UInt8 flag = 0;
        readBinaryLittleEndian(flag, buf);
        has_sample = flag != 0;
        readBinaryLittleEndian(min_val, buf);
        readBinaryLittleEndian(max_val, buf);
    }
};

/// Bucket for `first_over_time` / `ts_of_first_over_time`: stores only the sample with the
/// MIN timestamp in the bucket (mirrors `TimeseriesOverTimeSingleSampleBucket` which keeps
/// the LAST). Driven by the two-pointer-left aligned driver.
template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeFirstSampleBucket
{
    using TimestampType = TimestampTypeT;
    using ValueType = ValueTypeT;

    TimestampType min_ts{};
    ValueType value_at_min_ts{};
    bool has_sample{false};

    void add(TimestampType timestamp, ValueType value)
    {
        if (!has_sample || timestamp < min_ts)
        {
            min_ts = timestamp;
            value_at_min_ts = value;
            has_sample = true;
        }
        else if (timestamp == min_ts)
        {
            /// PromQL same-`ts` dedup: keep max value at that ts.
            value_at_min_ts = std::max(value_at_min_ts, value);
        }
    }

    void merge(const TimeseriesOverTimeFirstSampleBucket & other)
    {
        if (other.has_sample)
            add(other.min_ts, other.value_at_min_ts);
    }

    void flushDedup() const {}

    void serialize(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(static_cast<UInt8>(has_sample ? 1 : 0), buf);
        if (has_sample)
        {
            writeBinaryLittleEndian(min_ts, buf);
            writeBinaryLittleEndian(value_at_min_ts, buf);
        }
    }

    template <typename CheckTimestampInRange>
    void deserialize(ReadBuffer & buf, CheckTimestampInRange && check_timestamp_in_range)
    {
        has_sample = false;
        min_ts = {};
        value_at_min_ts = {};

        UInt8 flag = 0;
        readBinaryLittleEndian(flag, buf);
        if (flag)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            check_timestamp_in_range(timestamp);

            ValueType value;
            readBinaryLittleEndian(value, buf);

            min_ts = timestamp;
            value_at_min_ts = value;
            has_sample = true;
        }
    }
};

/// Bucket for `ts_of_min_over_time` / `ts_of_max_over_time`: per-bucket extrema together
/// with the timestamps at which they occur. Matches the VictoriaMetrics
/// `rollupTmin / rollupTmax` tie-break — when several samples in a bucket share the extreme
/// value, the **latest** ts wins (per-bucket `<=` / `>=` updates). The mono-deque driver
/// likewise prefers the later bucket on ties so the global last-ts-at-extreme is preserved.
template <typename TimestampTypeT, typename ValueTypeT>
struct TimeseriesOverTimeTsAtExtremeStatsBucket
{
    using TimestampType = TimestampTypeT;
    using ValueType = ValueTypeT;

    bool has_sample = false;
    ValueType min_val{};
    ValueType max_val{};
    TimestampType ts_at_min{};
    TimestampType ts_at_max{};

    void add(TimestampType timestamp, ValueType value)
    {
        if (!has_sample)
        {
            min_val = max_val = value;
            ts_at_min = ts_at_max = timestamp;
            has_sample = true;
            return;
        }
        if (value < min_val || (value == min_val && timestamp >= ts_at_min))
        {
            min_val = value;
            ts_at_min = timestamp;
        }
        if (value > max_val || (value == max_val && timestamp >= ts_at_max))
        {
            max_val = value;
            ts_at_max = timestamp;
        }
    }

    void merge(const TimeseriesOverTimeTsAtExtremeStatsBucket & other)
    {
        if (!other.has_sample)
            return;
        if (!has_sample)
        {
            *this = other;
            return;
        }
        if (other.min_val < min_val || (other.min_val == min_val && other.ts_at_min >= ts_at_min))
        {
            min_val = other.min_val;
            ts_at_min = other.ts_at_min;
        }
        if (other.max_val > max_val || (other.max_val == max_val && other.ts_at_max >= ts_at_max))
        {
            max_val = other.max_val;
            ts_at_max = other.ts_at_max;
        }
    }

    void flushDedup() const {}

    void serialize(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(static_cast<UInt8>(has_sample ? 1 : 0), buf);
        if (has_sample)
        {
            writeBinaryLittleEndian(min_val, buf);
            writeBinaryLittleEndian(max_val, buf);
            writeBinaryLittleEndian(ts_at_min, buf);
            writeBinaryLittleEndian(ts_at_max, buf);
        }
    }

    template <typename CheckTimestampInRange>
    void deserialize(ReadBuffer & buf, CheckTimestampInRange && check_timestamp_in_range)
    {
        has_sample = false;
        min_val = max_val = ValueType{};
        ts_at_min = ts_at_max = TimestampType{};

        UInt8 flag = 0;
        readBinaryLittleEndian(flag, buf);
        if (flag)
        {
            readBinaryLittleEndian(min_val, buf);
            readBinaryLittleEndian(max_val, buf);
            readBinaryLittleEndian(ts_at_min, buf);
            readBinaryLittleEndian(ts_at_max, buf);
            check_timestamp_in_range(ts_at_min);
            check_timestamp_in_range(ts_at_max);
            has_sample = true;
        }
    }
};

}
