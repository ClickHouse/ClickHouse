#pragma once

#include <cmath>
#include <cstddef>
#include <optional>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

/// Which sample's timestamp a `timeSeriesTsOf*ToGrid` aggregate returns for a window.
enum class TimeseriesTsOfKind
{
    First,  /// timestamp of the earliest sample (smallest timestamp)          -> ts_of_first_over_time
    Last,   /// timestamp of the latest sample (largest timestamp)             -> ts_of_last_over_time
    Min,    /// timestamp of the minimum-value sample (latest one on ties)     -> ts_of_min_over_time
    Max,    /// timestamp of the maximum-value sample (latest one on ties)     -> ts_of_max_over_time
};

template <typename TimestampType_, typename ValueType_, TimeseriesTsOfKind kind_>
struct AggregateFunctionTimeseriesTsOfToGridTraits
{
    static constexpr TimeseriesTsOfKind kind = kind_;

    using GridScaleTimestampType = DateTime64;
    using TimestampType = TimestampType_;
    using ValueType = ValueType_;
    /// A Unix-seconds timestamp does not fit Float32 exactly (e.g. 1699999940 rounds to 1700000000),
    /// so the result is always Float64 regardless of the sample value type.
    using ResultType = Float64;

    static String getName()
    {
        switch (kind_)
        {
            case TimeseriesTsOfKind::First: return "timeSeriesTsOfFirstToGrid";
            case TimeseriesTsOfKind::Last:  return "timeSeriesTsOfLastToGrid";
            case TimeseriesTsOfKind::Min:   return "timeSeriesTsOfMinToGrid";
            case TimeseriesTsOfKind::Max:   return "timeSeriesTsOfMaxToGrid";
        }
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Per-bucket (and, once combined, per-window) summary that keeps the single sample selected by `kind`:
    ///   - First/Last: the sample with the smallest/largest timestamp (value is irrelevant).
    ///   - Min/Max:    the sample with the smallest/largest value; NaN values lose to any real value, and on
    ///                 ties the latest (largest) timestamp wins, matching Prometheus `compareOverTime` (which
    ///                 scans in ascending time using `<=`/`>=`, so the last extreme sample is kept, and treats a
    ///                 running NaN as replaceable by any real value).
    /// `merge` is commutative and associative, so combining buckets in any order yields the same selection. It
    /// has no `unmerge` (a min/max/extreme cannot be subtracted), so `SlidingSum` recomputes per grid point.
    struct Summary
    {
        bool has_value = false;
        TimestampType timestamp = 0;
        ValueType value = 0;

        void merge(const Summary & other)
        {
            if (!other.has_value)
                return;
            if (!has_value)
            {
                *this = other;
                return;
            }

            if constexpr (kind_ == TimeseriesTsOfKind::First)
            {
                if (other.timestamp < timestamp)
                    *this = other;
            }
            else if constexpr (kind_ == TimeseriesTsOfKind::Last)
            {
                if (other.timestamp > timestamp)
                    *this = other;
            }
            else
            {
                const bool this_nan = std::isnan(static_cast<double>(value));
                const bool other_nan = std::isnan(static_cast<double>(other.value));

                if (this_nan != other_nan)
                {
                    /// A real value always beats NaN.
                    if (this_nan)
                        *this = other;
                    return;
                }

                if (this_nan && other_nan)
                {
                    /// All-NaN window: Prometheus ends on the last scanned sample, i.e. the largest timestamp.
                    if (other.timestamp > timestamp)
                        *this = other;
                    return;
                }

                /// Both real: pick the extreme value; on ties keep the latest (largest) timestamp.
                bool take = false;
                if constexpr (kind_ == TimeseriesTsOfKind::Min)
                    take = (other.value < value) || (other.value == value && other.timestamp > timestamp);
                else
                    take = (other.value > value) || (other.value == value && other.timestamp > timestamp);
                if (take)
                    *this = other;
            }
        }
    };

    /// Sliding aggregator: keeps the window's selected sample in a `SlidingSum` and returns its timestamp
    /// (converted to Unix seconds, i.e. divided by the number of ticks per second of the input timestamps) per grid point.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<Summary> sliding_sum;
        Int64 column_ticks_per_second;

        explicit Aggregator(Int64 column_ticks_per_second_)
            : column_ticks_per_second(column_ticks_per_second_)
        {
        }

        void add(const Samples & samples, GridScaleTimestampType bucket_end_timestamp)
        {
            Summary summary;
            samples.forEachSample([&summary](TimestampType timestamp, ValueType value)
            {
                Summary one;
                one.has_value = true;
                one.timestamp = timestamp;
                one.value = value;
                summary.merge(one);
            });
            add(std::move(summary), bucket_end_timestamp);
        }

        void add(Summary summary, GridScaleTimestampType bucket_end_timestamp)
        {
            if (!summary.has_value)
                return;
            sliding_sum.add(std::move(summary), bucket_end_timestamp);
        }

        void removeBefore(GridScaleTimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<ResultType> getResult(GridScaleTimestampType /*grid_timestamp*/) const
        {
            const Summary combined = sliding_sum.getCurrentSum();
            if (!combined.has_value)
                return std::nullopt;
            return static_cast<Float64>(static_cast<Int64>(combined.timestamp)) / static_cast<Float64>(column_ticks_per_second);
        }
    };

    /// The bucket stores raw samples; the aggregator's `add(const Samples &)` selects the relevant sample.
    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 1;
};


/// Aggregate function that returns the timestamp (in Unix seconds) of a selected sample of a time series within a
/// sliding window on a regular time grid (Prometheus `ts_of_first/last/min/max_over_time`).
template <typename TimestampType_, typename ValueType_, TimeseriesTsOfKind kind_>
class AggregateFunctionTimeseriesTsOfToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesTsOfToGrid<TimestampType_, ValueType_, kind_>,
        AggregateFunctionTimeseriesTsOfToGridTraits<TimestampType_, ValueType_, kind_>>
{
public:
    using Traits = AggregateFunctionTimeseriesTsOfToGridTraits<TimestampType_, ValueType_, kind_>;

    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesTsOfToGrid, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t /* num_populated_buckets */) const
    {
        return Aggregator{Base::column_ticks_per_second};
    }

    static constexpr bool DateTime64Supported = true;
};

/// Each SQL function as a 2-argument template with its `kind` baked in, so registration names the function directly.
template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesTsOfFirstToGrid = AggregateFunctionTimeseriesTsOfToGrid<TimestampType, ValueType, TimeseriesTsOfKind::First>;

template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesTsOfLastToGrid = AggregateFunctionTimeseriesTsOfToGrid<TimestampType, ValueType, TimeseriesTsOfKind::Last>;

template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesTsOfMinToGrid = AggregateFunctionTimeseriesTsOfToGrid<TimestampType, ValueType, TimeseriesTsOfKind::Min>;

template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesTsOfMaxToGrid = AggregateFunctionTimeseriesTsOfToGrid<TimestampType, ValueType, TimeseriesTsOfKind::Max>;

}
