#pragma once

#include <cstddef>
#include <optional>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

template <typename TimestampType_, typename IntervalType_, typename ValueType_>
struct AggregateFunctionTimeseriesFirstToGridTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return "timeSeriesFirstToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Per-bucket summary that keeps the earliest (smallest timestamp) sample of the bucket, and (once combined
    /// over a window) of the window too. `merge` keeps the sample with the smaller timestamp. It has no `unmerge`
    /// (a minimum cannot be subtracted), so the `SlidingSum` recomputes the window's earliest sample per grid point.
    struct Summary
    {
        bool has_value = false;
        TimestampType timestamp = 0;
        ValueType value = 0;

        void merge(const Summary & other)
        {
            if (!other.has_value)
                return;
            if (!has_value || other.timestamp < timestamp)
            {
                has_value = true;
                timestamp = other.timestamp;
                value = other.value;
            }
        }
    };

    /// Sliding aggregator: keeps the window's earliest sample in a `SlidingSum` and returns its value per grid point.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            Summary summary;
            samples.forEachSample([&summary](TimestampType timestamp, ValueType value)
            {
                if (!summary.has_value || timestamp < summary.timestamp)
                {
                    summary.has_value = true;
                    summary.timestamp = timestamp;
                    summary.value = value;
                }
            });
            add(std::move(summary), bucket_end_timestamp);
        }

        void add(Summary summary, TimestampType bucket_end_timestamp)
        {
            if (!summary.has_value)
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
            if (!combined.has_value)
                return std::nullopt;
            return combined.value;
        }
    };

    /// The bucket stores raw samples; the aggregator's `add(const Samples &)` extracts the earliest sample.
    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 1;
};


/// Aggregate function that returns the earliest (smallest timestamp) value of a time series within a sliding
/// window on a regular time grid (Prometheus `first_over_time`).
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesFirstToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesFirstToGrid<TimestampType_, IntervalType_, ValueType_>,
        AggregateFunctionTimeseriesFirstToGridTraits<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesFirstToGridTraits<TimestampType_, IntervalType_, ValueType_>;

    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesFirstToGrid, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t /* num_populated_buckets */) const
    {
        return {};
    }

    static constexpr bool DateTime64Supported = true;
};

}
