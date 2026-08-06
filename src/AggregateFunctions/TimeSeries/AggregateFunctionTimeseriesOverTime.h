#pragma once

#include <cmath>
#include <cstddef>
#include <optional>

#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/// Implements the PromQL `*_over_time` family (max, min, avg, sum, count, stddev, stdvar, present) on a grid.
///
/// Unlike rate/increase/delta (which buffer raw samples per bucket in `AggregateFunctionTimeseriesSamples`)
/// these aggregations are decomposable, so each bucket keeps only an O(1) `Summary` (a commutative monoid:
/// a default-constructed summary is the identity and `merge` combines two summaries). Memory per series is
/// O(buckets), not O(samples) - the whole reason the family gets its own bucket type.
///
/// The per-window result is maintained by `AggregateFunctionTimeseriesSlidingSum` over the bucket summaries.
/// None of the policies define `unmerge` (subtracting floating-point summaries would accumulate error), so the
/// sliding sum uses the two-stacks monoid queue for dense windows and recompute for sparse ones - the same
/// policy switch as `AggregateFunctionTimeseriesLinearRegression`, requiring commutative `merge`, which all
/// the policies below satisfy.
///
/// Semantics follow Prometheus:
///  - a window is the left-open interval (grid_timestamp - window, grid_timestamp];
///  - an empty window produces NULL (the grid point is then dropped from the PromQL result);
///  - max/min ignore NaN samples unless every sample in the window is NaN (like funcMaxOverTime, which
///    replaces the running max when `v > max || isNaN(max)`);
///  - sum/avg propagate NaN (plain float addition);
///  - stddev/stdvar are population statistics, accumulated with Welford's algorithm and combined across
///    buckets with Chan's parallel merge (numerically stable, order-independent).
///
/// Known deviations (documented, acceptable for a first version):
///  - `avg_over_time`/`sum_over_time` use plain Float64 summation, not Prometheus's Kahan-compensated
///    incremental mean; results may differ in the last ULPs for pathological value ranges.
///  - duplicate samples (same series, same timestamp, e.g. from remote-write retries before merges dedup
///    them) are counted twice by count/avg/sum summaries, while the sample-buffering functions (rate etc.)
///    dedup by timestamp. Prometheus never stores duplicates, so this only matters for unmerged CH parts.
struct TimeseriesOverTimeMaxPolicy
{
    static constexpr const char * name = "timeSeriesMaxToGrid";

    struct Payload
    {
        Float64 value = 0;

        void add(Float64 v, UInt64 count_before)
        {
            if (count_before == 0 || v > value || std::isnan(value))
                value = v;
        }

        void merge(const Payload & other, UInt64 lhs_count, UInt64 rhs_count)
        {
            if (rhs_count == 0)
                return;
            if (lhs_count == 0 || other.value > value || std::isnan(value))
                value = other.value;
        }

        void serialize(WriteBuffer & buf) const { writeBinaryLittleEndian(value, buf); }
        void deserialize(ReadBuffer & buf) { readBinaryLittleEndian(value, buf); }

        static std::optional<Float64> getResult(const Payload & payload, UInt64 /*count*/) { return payload.value; }
    };
};

struct TimeseriesOverTimeMinPolicy
{
    static constexpr const char * name = "timeSeriesMinToGrid";

    struct Payload
    {
        Float64 value = 0;

        void add(Float64 v, UInt64 count_before)
        {
            if (count_before == 0 || v < value || std::isnan(value))
                value = v;
        }

        void merge(const Payload & other, UInt64 lhs_count, UInt64 rhs_count)
        {
            if (rhs_count == 0)
                return;
            if (lhs_count == 0 || other.value < value || std::isnan(value))
                value = other.value;
        }

        void serialize(WriteBuffer & buf) const { writeBinaryLittleEndian(value, buf); }
        void deserialize(ReadBuffer & buf) { readBinaryLittleEndian(value, buf); }

        static std::optional<Float64> getResult(const Payload & payload, UInt64 /*count*/) { return payload.value; }
    };
};

struct TimeseriesOverTimeSumPolicy
{
    static constexpr const char * name = "timeSeriesSumToGrid";

    struct Payload
    {
        Float64 sum = 0;

        void add(Float64 v, UInt64 /*count_before*/) { sum += v; }
        void merge(const Payload & other, UInt64 /*lhs_count*/, UInt64 /*rhs_count*/) { sum += other.sum; }

        void serialize(WriteBuffer & buf) const { writeBinaryLittleEndian(sum, buf); }
        void deserialize(ReadBuffer & buf) { readBinaryLittleEndian(sum, buf); }

        static std::optional<Float64> getResult(const Payload & payload, UInt64 /*count*/) { return payload.sum; }
    };
};

struct TimeseriesOverTimeAvgPolicy
{
    static constexpr const char * name = "timeSeriesAvgToGrid";

    struct Payload
    {
        Float64 sum = 0;

        void add(Float64 v, UInt64 /*count_before*/) { sum += v; }
        void merge(const Payload & other, UInt64 /*lhs_count*/, UInt64 /*rhs_count*/) { sum += other.sum; }

        void serialize(WriteBuffer & buf) const { writeBinaryLittleEndian(sum, buf); }
        void deserialize(ReadBuffer & buf) { readBinaryLittleEndian(sum, buf); }

        static std::optional<Float64> getResult(const Payload & payload, UInt64 count)
        {
            return payload.sum / static_cast<Float64>(count);
        }
    };
};

struct TimeseriesOverTimeCountPolicy
{
    static constexpr const char * name = "timeSeriesCountToGrid";

    struct Payload
    {
        void add(Float64 /*v*/, UInt64 /*count_before*/) {}
        void merge(const Payload & /*other*/, UInt64 /*lhs_count*/, UInt64 /*rhs_count*/) {}
        void serialize(WriteBuffer & /*buf*/) const {}
        void deserialize(ReadBuffer & /*buf*/) {}

        static std::optional<Float64> getResult(const Payload & /*payload*/, UInt64 count)
        {
            return static_cast<Float64>(count);
        }
    };
};

struct TimeseriesOverTimePresentPolicy
{
    static constexpr const char * name = "timeSeriesPresentToGrid";

    struct Payload
    {
        void add(Float64 /*v*/, UInt64 /*count_before*/) {}
        void merge(const Payload & /*other*/, UInt64 /*lhs_count*/, UInt64 /*rhs_count*/) {}
        void serialize(WriteBuffer & /*buf*/) const {}
        void deserialize(ReadBuffer & /*buf*/) {}

        static std::optional<Float64> getResult(const Payload & /*payload*/, UInt64 /*count*/) { return 1.0; }
    };
};

/// Centered moments for stddev/stdvar (population): Welford accumulation within a bucket, Chan's parallel
/// merge across buckets - the same scheme (and the same numerical-stability argument) as the x/y moments in
/// `AggregateFunctionTimeseriesLinearRegressionTraits::Summary`.
template <bool is_stddev>
struct TimeseriesOverTimeMomentsPolicy
{
    static constexpr const char * name = is_stddev ? "timeSeriesStddevToGrid" : "timeSeriesStdvarToGrid";

    struct Payload
    {
        Float64 mean = 0;
        Float64 m2 = 0;

        void add(Float64 v, UInt64 count_before)
        {
            const Float64 count = static_cast<Float64>(count_before) + 1;
            const Float64 dv = v - mean;
            mean += dv / count;
            m2 += dv * (v - mean);
        }

        void merge(const Payload & other, UInt64 lhs_count, UInt64 rhs_count)
        {
            if (rhs_count == 0)
                return;
            if (lhs_count == 0)
            {
                *this = other;
                return;
            }
            const Float64 na = static_cast<Float64>(lhs_count);
            const Float64 nb = static_cast<Float64>(rhs_count);
            const Float64 total = na + nb;
            const Float64 dv = other.mean - mean;
            mean += dv * nb / total;
            m2 += other.m2 + dv * dv * na * nb / total;
        }

        void serialize(WriteBuffer & buf) const
        {
            writeBinaryLittleEndian(mean, buf);
            writeBinaryLittleEndian(m2, buf);
        }

        void deserialize(ReadBuffer & buf)
        {
            readBinaryLittleEndian(mean, buf);
            readBinaryLittleEndian(m2, buf);
        }

        static std::optional<Float64> getResult(const Payload & payload, UInt64 count)
        {
            const Float64 variance = payload.m2 / static_cast<Float64>(count);
            return is_stddev ? std::sqrt(variance) : variance;
        }
    };
};


template <typename TimestampType_, typename IntervalType_, typename ValueType_, typename Policy_>
struct AggregateFunctionTimeseriesOverTimeTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;
    using Policy = Policy_;
    using Payload = typename Policy::Payload;

    static String getName() { return Policy::name; }

    /// Per-bucket summary and, once combined over a window, the window summary too. O(1) size.
    /// `latest_timestamp` carries the newest sample's timestamp for post-deserialization validation only
    /// (`checkTimestampsInRange`); the aggregation itself never reads it.
    struct Summary
    {
        Payload payload{};
        TimestampType latest_timestamp{};
        UInt64 count = 0;

        void add(TimestampType timestamp, ValueType value)
        {
            payload.add(static_cast<Float64>(value), count);
            if (count == 0 || timestamp > latest_timestamp)
                latest_timestamp = timestamp;
            ++count;
        }

        void merge(const Summary & other)
        {
            if (other.count == 0)
                return;
            payload.merge(other.payload, count, other.count);
            if (count == 0 || other.latest_timestamp > latest_timestamp)
                latest_timestamp = other.latest_timestamp;
            count += other.count;
        }

        void serialize(WriteBuffer & buf) const
        {
            writeBinaryLittleEndian(count, buf);
            writeBinaryLittleEndian(latest_timestamp, buf);
            payload.serialize(buf);
        }

        void deserialize(ReadBuffer & buf)
        {
            readBinaryLittleEndian(count, buf);
            readBinaryLittleEndian(latest_timestamp, buf);
            payload.deserialize(buf);
        }

        template <typename RangeType>
        void checkTimestampsInRange(const RangeType & range) const
        {
            if (count && !range.contains(latest_timestamp))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Cannot deserialize data: timestamp {} is outside its bucket's range",
                    static_cast<Int64>(latest_timestamp));
        }
    };

    /// Sliding aggregator over the bucket summaries. Buckets are already preaggregated (Bucket == Summary),
    /// so `add` only forwards a copy into the sliding sum.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;

        explicit Aggregator(size_t stack_size) : sliding_sum(stack_size) {}

        void add(const Summary & summary, TimestampType bucket_end_timestamp)
        {
            if (summary.count == 0)
                return;
            Summary copy = summary;
            sliding_sum.add(std::move(copy), bucket_end_timestamp);
        }

        void removeBefore(TimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/) const
        {
            const Summary combined = sliding_sum.getCurrentSum();
            if (combined.count == 0)
                return std::nullopt;
            const auto result = Payload::getResult(combined.payload, combined.count);
            if (!result)
                return std::nullopt;
            return static_cast<ValueType>(*result);
        }
    };

    /// The bucket IS the summary: O(1) per bucket, no raw-sample buffering.
    using Bucket = Summary;
};


template <typename TimestampType_, typename IntervalType_, typename ValueType_, typename Policy_>
class AggregateFunctionTimeseriesOverTime final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesOverTime<TimestampType_, IntervalType_, ValueType_, Policy_>,
        AggregateFunctionTimeseriesOverTimeTraits<TimestampType_, IntervalType_, ValueType_, Policy_>>
{
public:
    using Traits = AggregateFunctionTimeseriesOverTimeTraits<TimestampType_, IntervalType_, ValueType_, Policy_>;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTime, Traits>;
    using Base::Base;

    /// The two-stacks/recompute switch and its thresholds are shared with
    /// `AggregateFunctionTimeseriesLinearRegression` (see the discussion there; measured by the
    /// `timeseries_to_grid_two_stack_vs_recompute` example). Summaries here are even cheaper to combine than
    /// regression moments, which only shifts the crossover in recompute's favor - the thresholds stay valid.
    static constexpr size_t AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS = 10;
    static constexpr size_t BPW_TO_FORCE_TWO_STACKS = 20;

    Aggregator createAggregator(size_t num_populated_buckets) const
    {
        const size_t avg_buckets_in_window = Base::bucket_count
            ? static_cast<size_t>(static_cast<double>(Base::buckets_per_window) * static_cast<double>(num_populated_buckets)
                / static_cast<double>(Base::bucket_count))
            : 0;
        const bool use_two_stacks = avg_buckets_in_window >= AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS
            || Base::buckets_per_window >= BPW_TO_FORCE_TWO_STACKS;
        const size_t stack_size = use_two_stacks ? std::min(Base::buckets_per_window, num_populated_buckets) : 0;
        return Aggregator{stack_size};
    }

    static constexpr UInt16 FORMAT_VERSION = 1;
    static constexpr bool DateTime64Supported = true;
};


/// One alias per SQL function, so registration names the function directly.
template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesMaxToGrid = AggregateFunctionTimeseriesOverTime<TimestampType, IntervalType, ValueType, TimeseriesOverTimeMaxPolicy>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesMinToGrid = AggregateFunctionTimeseriesOverTime<TimestampType, IntervalType, ValueType, TimeseriesOverTimeMinPolicy>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesSumToGrid = AggregateFunctionTimeseriesOverTime<TimestampType, IntervalType, ValueType, TimeseriesOverTimeSumPolicy>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesAvgToGrid = AggregateFunctionTimeseriesOverTime<TimestampType, IntervalType, ValueType, TimeseriesOverTimeAvgPolicy>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesCountToGrid = AggregateFunctionTimeseriesOverTime<TimestampType, IntervalType, ValueType, TimeseriesOverTimeCountPolicy>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesPresentToGrid = AggregateFunctionTimeseriesOverTime<TimestampType, IntervalType, ValueType, TimeseriesOverTimePresentPolicy>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesStddevToGrid = AggregateFunctionTimeseriesOverTime<TimestampType, IntervalType, ValueType, TimeseriesOverTimeMomentsPolicy<true>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesStdvarToGrid = AggregateFunctionTimeseriesOverTime<TimestampType, IntervalType, ValueType, TimeseriesOverTimeMomentsPolicy<false>>;

}
