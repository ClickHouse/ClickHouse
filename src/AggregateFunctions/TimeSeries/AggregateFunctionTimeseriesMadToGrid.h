#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <optional>
#include <vector>

#include <Common/VectorWithMemoryTracking.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

template <typename TimestampType_, typename IntervalType_, typename ValueType_>
struct AggregateFunctionTimeseriesMadToGridTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return "timeSeriesMadToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// R-7 (inclusive) quantile of a sorted, non-empty range, matching Prometheus's `quantile()` helper
    /// and `quantileExactInclusive`.
    static Float64 quantileR7Sorted(const std::vector<ValueType> & sorted, Float64 phi) // STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        const size_t n = sorted.size();
        const Float64 rank = phi * static_cast<Float64>(n - 1);
        const size_t lower = static_cast<size_t>(std::floor(rank));
        const size_t upper = static_cast<size_t>(std::ceil(rank));
        if (lower == upper)
            return static_cast<Float64>(sorted[lower]);

        const Float64 fraction = rank - static_cast<Float64>(lower);
        return static_cast<Float64>(sorted[lower])
            + fraction * (static_cast<Float64>(sorted[upper]) - static_cast<Float64>(sorted[lower]));
    }

    /// Per-bucket summary that collects the bucket's values, and (once combined over a window) the window's
    /// values too. It has no `unmerge`, so the `SlidingSum` recomputes the window's values per grid point.
    struct Summary
    {
        VectorWithMemoryTracking<ValueType> values;

        void merge(const Summary & other)
        {
            values.insert(values.end(), other.values.begin(), other.values.end());
        }
    };

    /// Sliding aggregator: keeps the window's values in a `SlidingSum` and computes the median absolute
    /// deviation `median(|x - median(x)|)` per grid point.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            Summary summary;
            samples.forEachSample([&summary](TimestampType, ValueType value)
            {
                summary.values.push_back(value);
            });
            add(std::move(summary), bucket_end_timestamp);
        }

        void add(Summary summary, TimestampType bucket_end_timestamp)
        {
            if (summary.values.empty())
                return;
            sliding_sum.add(std::move(summary), bucket_end_timestamp);
        }

        void removeBefore(TimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/) const
        {
            const Summary combined = sliding_sum.getCurrentSum();
            if (combined.values.empty())
                return std::nullopt;

            std::vector<ValueType> sorted_values(combined.values.begin(), combined.values.end()); // STYLE_CHECK_ALLOW_STD_CONTAINERS

            /// A NaN sample makes the median (and hence the deviation) undefined, so propagate NaN rather than
            /// silently dropping it (matches Prometheus `mad_over_time`).
            for (const auto v : sorted_values)
            {
                if (std::isnan(static_cast<Float64>(v)))
                    return static_cast<ValueType>(std::numeric_limits<Float64>::quiet_NaN());
            }

            std::sort(sorted_values.begin(), sorted_values.end());
            const Float64 median = quantileR7Sorted(sorted_values, 0.5);

            std::vector<ValueType> deviations; // STYLE_CHECK_ALLOW_STD_CONTAINERS
            deviations.reserve(sorted_values.size());
            for (const auto v : sorted_values)
                deviations.push_back(static_cast<ValueType>(std::abs(static_cast<Float64>(v) - median)));

            std::sort(deviations.begin(), deviations.end());
            return static_cast<ValueType>(quantileR7Sorted(deviations, 0.5));
        }
    };

    /// The bucket stores raw samples; the aggregator's `add(const Samples &)` collects their values.
    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 1;
};


/// Aggregate function that computes the median absolute deviation of time series values over a sliding window on
/// a regular time grid (Prometheus `mad_over_time`).
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesMadToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesMadToGrid<TimestampType_, IntervalType_, ValueType_>,
        AggregateFunctionTimeseriesMadToGridTraits<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesMadToGridTraits<TimestampType_, IntervalType_, ValueType_>;

    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesMadToGrid, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t /* num_populated_buckets */) const
    {
        return {};
    }

    static constexpr bool DateTime64Supported = true;
};

}
