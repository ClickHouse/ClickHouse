#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <optional>
#include <type_traits>
#include <base/types.h>

#include <Common/VectorWithMemoryTracking.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

class AggregateFunctionFactory;

namespace TimeseriesOverTimeDetails
{

/// Mirrors Prometheus `promql/quantile.go` `quantile`.
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

enum class Kind
{
    Avg,
    Min,
    Max,
    Sum,
    Count,
    Stddev,
    Stdvar,
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

/// Running sum and count (`sum_over_time`). Invertible via `unmerge`.
/// `count` distinguishes an empty window (no value) from a non-empty window whose sum is zero.
template <typename TimestampType, typename ValueType>
struct SumSummary
{
    UInt64 count = 0;
    Float64 sum = 0;

    void addSample(TimestampType /*ts*/, ValueType value)
    {
        ++count;
        sum += static_cast<Float64>(value);
    }

    void merge(const SumSummary & other)
    {
        count += other.count;
        sum += other.sum;
    }

    void unmerge(const SumSummary & leaving, const SumSummary * /*new_first*/)
    {
        count -= leaving.count;
        sum -= leaving.sum;
    }
};

/// Running sum and count (`avg_over_time`). Invertible via `unmerge`.
template <typename TimestampType, typename ValueType>
struct AvgSummary
{
    UInt64 count = 0;
    Float64 sum = 0;

    void addSample(TimestampType /*ts*/, ValueType value)
    {
        ++count;
        sum += static_cast<Float64>(value);
    }

    void merge(const AvgSummary & other)
    {
        count += other.count;
        sum += other.sum;
    }

    void unmerge(const AvgSummary & leaving, const AvgSummary * /*new_first*/)
    {
        count -= leaving.count;
        sum -= leaving.sum;
    }
};

/// Running moments for stddev / stdvar. Invertible via `unmerge`.
template <typename TimestampType, typename ValueType>
struct StatsSummary
{
    UInt64 count = 0;
    Float64 sum = 0;
    Float64 sum_sq = 0;

    void addSample(TimestampType /*ts*/, ValueType value)
    {
        const Float64 v = static_cast<Float64>(value);
        ++count;
        sum += v;
        sum_sq += v * v;
    }

    void merge(const StatsSummary & other)
    {
        count += other.count;
        sum += other.sum;
        sum_sq += other.sum_sq;
    }

    void unmerge(const StatsSummary & leaving, const StatsSummary * /*new_first*/)
    {
        count -= leaving.count;
        sum -= leaving.sum;
        sum_sq -= leaving.sum_sq;
    }
};

template <typename TimestampType, typename ValueType, bool is_max>
struct ExtremumSummary
{
    bool has = false;
    ValueType value{};

    void addSample(TimestampType /*ts*/, ValueType sample)
    {
        if (!has)
        {
            value = sample;
            has = true;
            return;
        }
        if constexpr (is_max)
            value = std::max(value, sample);
        else
            value = std::min(value, sample);
    }

    void merge(const ExtremumSummary & other)
    {
        if (!other.has)
            return;
        if (!has)
        {
            *this = other;
            return;
        }
        if constexpr (is_max)
            value = std::max(value, other.value);
        else
            value = std::min(value, other.value);
    }
};

template <typename TimestampType, typename ValueType>
struct CountSummary
{
    UInt64 count = 0;

    void addSample(TimestampType /*ts*/, ValueType /*value*/)
    {
        ++count;
    }

    void merge(const CountSummary & other)
    {
        count += other.count;
    }

    void unmerge(const CountSummary & leaving, const CountSummary * /*new_first*/)
    {
        count -= leaving.count;
    }
};

/// Earliest sample in the window. `merge` appends later buckets without changing the first sample;
/// `unmerge` restores the first from the new front bucket (invertible path).
template <typename TimestampType, typename ValueType>
struct FirstSummary
{
    bool has = false;
    TimestampType timestamp{};
    ValueType value{};

    void addSample(TimestampType ts, ValueType sample)
    {
        if (!has || ts < timestamp || (ts == timestamp && sample > value))
        {
            timestamp = ts;
            value = sample;
            has = true;
        }
    }

    void merge(const FirstSummary & other)
    {
        if (!other.has)
            return;
        if (!has)
        {
            *this = other;
            return;
        }
        /// Buckets are merged in ascending time order on the invertible path, so the existing first stays.
    }

    void unmerge(const FirstSummary & /*leaving*/, const FirstSummary * new_first)
    {
        if (new_first && new_first->has)
            *this = *new_first;
        else
            has = false;
    }
};

/// Latest sample in the window. Custom aggregator (no SlidingSum): buckets arrive in time order, so each
/// non-empty bucket's latest sample replaces the previous one; `removeBefore` clears once that sample is out.
template <typename TimestampType, typename ValueType>
struct LastSummary
{
    bool has = false;
    TimestampType timestamp{};
    ValueType value{};

    void addSample(TimestampType ts, ValueType sample)
    {
        if (!has || ts > timestamp || (ts == timestamp && sample > value))
        {
            timestamp = ts;
            value = sample;
            has = true;
        }
    }

    void merge(const LastSummary & other)
    {
        if (other.has)
            addSample(other.timestamp, other.value);
    }
};

template <typename TimestampType, typename ValueType, bool is_max>
struct TsOfExtremumSummary
{
    bool has = false;
    TimestampType timestamp{};
    ValueType value{};

    void addSample(TimestampType ts, ValueType sample)
    {
        if (!has)
        {
            timestamp = ts;
            value = sample;
            has = true;
            return;
        }

        if constexpr (is_max)
        {
            /// VictoriaMetrics `rollupTmax`: last timestamp for the maximum value.
            if (sample > value || (sample == value && ts > timestamp))
            {
                timestamp = ts;
                value = sample;
            }
        }
        else
        {
            /// VictoriaMetrics `rollupTmin`: last timestamp for the minimum value.
            if (sample < value || (sample == value && ts > timestamp))
            {
                timestamp = ts;
                value = sample;
            }
        }
    }

    void merge(const TsOfExtremumSummary & other)
    {
        if (other.has)
            addSample(other.timestamp, other.value);
    }
};

template <typename TimestampType, typename ValueType>
struct ValuesSummary
{
    VectorWithMemoryTracking<Float64> values;

    void addSample(TimestampType /*ts*/, ValueType sample)
    {
        values.push_back(static_cast<Float64>(sample));
    }

    void merge(const ValuesSummary & other)
    {
        if (other.values.empty())
            return;
        values.reserve(values.size() + other.values.size());
        values.insert(values.end(), other.values.begin(), other.values.end());
    }
};

template <Kind kind>
consteval const char * functionName()
{
    switch (kind)
    {
        case Kind::Avg: return "timeSeriesAvgOverTimeToGrid";
        case Kind::Min: return "timeSeriesMinOverTimeToGrid";
        case Kind::Max: return "timeSeriesMaxOverTimeToGrid";
        case Kind::Sum: return "timeSeriesSumOverTimeToGrid";
        case Kind::Count: return "timeSeriesCountOverTimeToGrid";
        case Kind::Stddev: return "timeSeriesStddevOverTimeToGrid";
        case Kind::Stdvar: return "timeSeriesStdvarOverTimeToGrid";
        case Kind::Present: return "timeSeriesPresentOverTimeToGrid";
        case Kind::Absent: return "timeSeriesAbsentOverTimeToGrid";
        case Kind::First: return "timeSeriesFirstOverTimeToGrid";
        case Kind::TsOfLast: return "timeSeriesTsOfLastOverTimeToGrid";
        case Kind::TsOfFirst: return "timeSeriesTsOfFirstOverTimeToGrid";
        case Kind::TsOfMin: return "timeSeriesTsOfMinOverTimeToGrid";
        case Kind::TsOfMax: return "timeSeriesTsOfMaxOverTimeToGrid";
        case Kind::Quantile: return "timeSeriesQuantileOverTimeToGrid";
        case Kind::Mad: return "timeSeriesMadOverTimeToGrid";
    }
}

template <Kind kind, typename TimestampType, typename ValueType>
struct ResultFromSummary;

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::Avg, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const AvgSummary<TimestampType, ValueType> & summary, TimestampType)
    {
        if (summary.count == 0)
            return std::nullopt;
        return static_cast<ValueType>(summary.sum / static_cast<Float64>(summary.count));
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::Sum, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const SumSummary<TimestampType, ValueType> & summary, TimestampType)
    {
        if (summary.count == 0)
            return std::nullopt;
        return static_cast<ValueType>(summary.sum);
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::Count, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const CountSummary<TimestampType, ValueType> & summary, TimestampType)
    {
        if (summary.count == 0)
            return std::nullopt;
        return static_cast<ValueType>(summary.count);
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::Stddev, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const StatsSummary<TimestampType, ValueType> & summary, TimestampType)
    {
        if (summary.count == 0)
            return std::nullopt;
        const Float64 mean = summary.sum / static_cast<Float64>(summary.count);
        const Float64 variance = std::max(0.0, summary.sum_sq / static_cast<Float64>(summary.count) - mean * mean);
        return static_cast<ValueType>(std::sqrt(variance));
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::Stdvar, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const StatsSummary<TimestampType, ValueType> & summary, TimestampType)
    {
        if (summary.count == 0)
            return std::nullopt;
        const Float64 mean = summary.sum / static_cast<Float64>(summary.count);
        const Float64 variance = std::max(0.0, summary.sum_sq / static_cast<Float64>(summary.count) - mean * mean);
        return static_cast<ValueType>(variance);
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::Min, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const ExtremumSummary<TimestampType, ValueType, false> & summary, TimestampType)
    {
        if (!summary.has)
            return std::nullopt;
        return summary.value;
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::Max, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const ExtremumSummary<TimestampType, ValueType, true> & summary, TimestampType)
    {
        if (!summary.has)
            return std::nullopt;
        return summary.value;
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::Present, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const CountSummary<TimestampType, ValueType> & summary, TimestampType)
    {
        if (summary.count == 0)
            return std::nullopt;
        return static_cast<ValueType>(1);
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::Absent, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const CountSummary<TimestampType, ValueType> & summary, TimestampType)
    {
        if (summary.count == 0)
            return static_cast<ValueType>(1);
        return std::nullopt;
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::First, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const FirstSummary<TimestampType, ValueType> & summary, TimestampType)
    {
        if (!summary.has)
            return std::nullopt;
        return summary.value;
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::TsOfFirst, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const FirstSummary<TimestampType, ValueType> & summary, TimestampType scale)
    {
        if (!summary.has)
            return std::nullopt;
        return static_cast<ValueType>(static_cast<Float64>(summary.timestamp) / static_cast<Float64>(scale));
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::TsOfLast, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const LastSummary<TimestampType, ValueType> & summary, TimestampType scale)
    {
        if (!summary.has)
            return std::nullopt;
        return static_cast<ValueType>(static_cast<Float64>(summary.timestamp) / static_cast<Float64>(scale));
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::TsOfMin, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const TsOfExtremumSummary<TimestampType, ValueType, false> & summary, TimestampType scale)
    {
        if (!summary.has)
            return std::nullopt;
        return static_cast<ValueType>(static_cast<Float64>(summary.timestamp) / static_cast<Float64>(scale));
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::TsOfMax, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const TsOfExtremumSummary<TimestampType, ValueType, true> & summary, TimestampType scale)
    {
        if (!summary.has)
            return std::nullopt;
        return static_cast<ValueType>(static_cast<Float64>(summary.timestamp) / static_cast<Float64>(scale));
    }
};

template <typename TimestampType, typename ValueType>
struct ResultFromSummary<Kind::Mad, TimestampType, ValueType>
{
    static std::optional<ValueType> apply(
        const ValuesSummary<TimestampType, ValueType> & summary, TimestampType)
    {
        if (summary.values.empty())
            return std::nullopt;
        VectorWithMemoryTracking<Float64> values = summary.values;
        const Float64 median = quantile(0.5, values);
        VectorWithMemoryTracking<Float64> abs_devs;
        abs_devs.reserve(values.size());
        for (Float64 v : values)
            abs_devs.push_back(std::abs(v - median));
        return static_cast<ValueType>(quantile(0.5, abs_devs));
    }
};

/// SlidingSum-backed aggregator for summaries that are monoids (invertible or two-stacks/recompute).
template <Kind kind, typename TimestampType, typename ValueType, typename Summary>
struct SlidingAggregator
{
    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;
    TimestampType timestamp_scale_multiplier{};

    SlidingAggregator(size_t stack_size, TimestampType timestamp_scale_multiplier_)
        : sliding_sum(stack_size)
        , timestamp_scale_multiplier(timestamp_scale_multiplier_)
    {
    }

    void add(const Samples & samples, TimestampType bucket_end_timestamp)
    {
        if (samples.empty())
            return;

        Summary summary;
        samples.forEachSample([&summary](TimestampType timestamp, ValueType value)
        {
            summary.addSample(timestamp, value);
        });
        sliding_sum.add(std::move(summary), bucket_end_timestamp);
    }

    void removeBefore(TimestampType cut_off)
    {
        sliding_sum.removeBefore(cut_off);
    }

    std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/) const
    {
        return ResultFromSummary<kind, TimestampType, ValueType>::apply(
            sliding_sum.getCurrentSum(), timestamp_scale_multiplier);
    }
};

/// Quantile-over-time: same sliding `ValuesSummary` window as mad, but result depends on `phi`.
template <typename TimestampType, typename ValueType>
struct QuantileAggregator
{
    using Summary = ValuesSummary<TimestampType, ValueType>;
    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;
    Float64 phi = 0;

    QuantileAggregator(size_t stack_size, Float64 phi_)
        : sliding_sum(stack_size)
        , phi(phi_)
    {
    }

    void add(const Samples & samples, TimestampType bucket_end_timestamp)
    {
        if (samples.empty())
            return;

        Summary summary;
        samples.forEachSample([&summary](TimestampType timestamp, ValueType value)
        {
            summary.addSample(timestamp, value);
        });
        sliding_sum.add(std::move(summary), bucket_end_timestamp);
    }

    void removeBefore(TimestampType cut_off)
    {
        sliding_sum.removeBefore(cut_off);
    }

    std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/) const
    {
        const Summary summary = sliding_sum.getCurrentSum();
        if (summary.values.empty())
            return std::nullopt;
        VectorWithMemoryTracking<Float64> values = summary.values;
        return static_cast<ValueType>(quantile(phi, values));
    }
};

/// Latest-sample aggregator for `ts_of_last_over_time` (same pattern as resample/last-to-grid).
template <typename TimestampType, typename ValueType>
struct LastAggregator
{
    using Summary = LastSummary<TimestampType, ValueType>;
    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    Summary latest;
    TimestampType timestamp_scale_multiplier{};

    explicit LastAggregator(TimestampType timestamp_scale_multiplier_)
        : timestamp_scale_multiplier(timestamp_scale_multiplier_)
    {
    }

    void add(const Samples & samples, TimestampType /*bucket_end_timestamp*/)
    {
        Summary summary;
        samples.forEachSample([&summary](TimestampType timestamp, ValueType value)
        {
            summary.addSample(timestamp, value);
        });
        latest.merge(summary);
    }

    void removeBefore(TimestampType cut_off)
    {
        if (latest.has && latest.timestamp <= cut_off)
            latest = Summary{};
    }

    std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/) const
    {
        return ResultFromSummary<Kind::TsOfLast, TimestampType, ValueType>::apply(
            latest, timestamp_scale_multiplier);
    }
};

}

template <
    typename TimestampType_,
    typename IntervalType_,
    typename ValueType_,
    TimeseriesOverTimeDetails::Kind kind_,
    typename Summary_>
struct AggregateFunctionTimeseriesOverTimeTraits
{
    static constexpr TimeseriesOverTimeDetails::Kind kind = kind_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;
    using Summary = Summary_;

    static String getName() { return TimeseriesOverTimeDetails::functionName<kind>(); }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;
    using Aggregator = std::conditional_t<
        kind == TimeseriesOverTimeDetails::Kind::TsOfLast,
        TimeseriesOverTimeDetails::LastAggregator<TimestampType, ValueType>,
        std::conditional_t<
            kind == TimeseriesOverTimeDetails::Kind::Quantile,
            TimeseriesOverTimeDetails::QuantileAggregator<TimestampType, ValueType>,
            TimeseriesOverTimeDetails::SlidingAggregator<kind, TimestampType, ValueType, Summary>>>;
    using Bucket = Samples;
};


template <typename Traits>
class AggregateFunctionTimeseriesOverTime final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTime<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;
    static constexpr UInt16 FORMAT_VERSION = 1;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTime<Traits>, Traits>;

    /// Two-stack vs recompute thresholds for non-invertible overtime summaries, measured by
    /// `timeseries_to_grid_two_stack_vs_recompute` (dense finalize sweep, ns/grid-point):
    ///   - ExtremumSummary (min/max):             enable @ 6, force @ 14 (2x)
    ///   - ValuesSummary (quantile/mad):          enable @ 6, force @ 14 (2x)
    ///   - TsOfExtremumSummary (ts_of_min/max):   enable @ 2, force @ 14 (2x)
    /// Invertible summaries (sum/avg/count/...) never consult these - they use `unmerge`.
    static constexpr size_t avgPopulatedBpwToEnableTwoStacks()
    {
        if constexpr (
            Traits::kind == TimeseriesOverTimeDetails::Kind::TsOfMin
            || Traits::kind == TimeseriesOverTimeDetails::Kind::TsOfMax)
            return 2;
        return 6;
    }

    static constexpr size_t BPW_TO_FORCE_TWO_STACKS = 14;

    /// `phi` is only used by `timeSeriesQuantileOverTimeToGrid`; other kinds leave it at 0.
    explicit AggregateFunctionTimeseriesOverTime(
        const DataTypes & argument_types_,
        const Array & parameters_,
        TimestampType start_timestamp_,
        TimestampType end_timestamp_,
        IntervalType step_,
        IntervalType window_,
        UInt32 timestamp_scale_,
        Float64 phi_ = 0)
        : Base(argument_types_, parameters_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , phi(phi_)
    {
    }

    Aggregator createAggregator(size_t num_populated_buckets) const
    {
        if constexpr (Traits::kind == TimeseriesOverTimeDetails::Kind::TsOfLast)
        {
            return Aggregator{Base::timestamp_scale_multiplier};
        }
        else
        {
            using SlidingSum = AggregateFunctionTimeseriesSlidingSum<TimestampType, typename Traits::Summary>;
            size_t stack_size = 0;
            if constexpr (!SlidingSum::is_invertible)
            {
                const size_t avg_buckets_in_window = Base::bucket_count
                    ? static_cast<size_t>(static_cast<double>(Base::buckets_per_window) * static_cast<double>(num_populated_buckets)
                        / static_cast<double>(Base::bucket_count))
                    : 0;
                const bool use_two_stacks = avg_buckets_in_window >= avgPopulatedBpwToEnableTwoStacks()
                    || Base::buckets_per_window >= BPW_TO_FORCE_TWO_STACKS;
                stack_size = use_two_stacks ? std::min(Base::buckets_per_window, num_populated_buckets) : 0;
            }
            if constexpr (Traits::kind == TimeseriesOverTimeDetails::Kind::Quantile)
                return Aggregator{stack_size, phi};
            else
                return Aggregator{stack_size, Base::timestamp_scale_multiplier};
        }
    }

private:
    Float64 phi = 0;
};


template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesAvgOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::Avg,
        TimeseriesOverTimeDetails::AvgSummary<TimestampType, ValueType>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesMinOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::Min,
        TimeseriesOverTimeDetails::ExtremumSummary<TimestampType, ValueType, false>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesMaxOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::Max,
        TimeseriesOverTimeDetails::ExtremumSummary<TimestampType, ValueType, true>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesSumOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::Sum,
        TimeseriesOverTimeDetails::SumSummary<TimestampType, ValueType>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesCountOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::Count,
        TimeseriesOverTimeDetails::CountSummary<TimestampType, ValueType>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesStddevOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::Stddev,
        TimeseriesOverTimeDetails::StatsSummary<TimestampType, ValueType>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesStdvarOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::Stdvar,
        TimeseriesOverTimeDetails::StatsSummary<TimestampType, ValueType>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesPresentOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::Present,
        TimeseriesOverTimeDetails::CountSummary<TimestampType, ValueType>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesAbsentOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::Absent,
        TimeseriesOverTimeDetails::CountSummary<TimestampType, ValueType>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesFirstOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::First,
        TimeseriesOverTimeDetails::FirstSummary<TimestampType, ValueType>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesTsOfLastOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::TsOfLast,
        TimeseriesOverTimeDetails::LastSummary<TimestampType, ValueType>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesTsOfFirstOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::TsOfFirst,
        TimeseriesOverTimeDetails::FirstSummary<TimestampType, ValueType>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesTsOfMinOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::TsOfMin,
        TimeseriesOverTimeDetails::TsOfExtremumSummary<TimestampType, ValueType, false>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesTsOfMaxOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::TsOfMax,
        TimeseriesOverTimeDetails::TsOfExtremumSummary<TimestampType, ValueType, true>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesQuantileOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::Quantile,
        TimeseriesOverTimeDetails::ValuesSummary<TimestampType, ValueType>>>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesMadOverTime = AggregateFunctionTimeseriesOverTime<
    AggregateFunctionTimeseriesOverTimeTraits<
        TimestampType,
        IntervalType,
        ValueType,
        TimeseriesOverTimeDetails::Kind::Mad,
        TimeseriesOverTimeDetails::ValuesSummary<TimestampType, ValueType>>>;


void registerAggregateFunctionTimeseriesOverTimeGrid(AggregateFunctionFactory & factory);

}
