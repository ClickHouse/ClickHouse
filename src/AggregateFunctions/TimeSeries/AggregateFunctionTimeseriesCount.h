#pragma once

#include <cstddef>
#include <optional>
#include <utility>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

/// Traits for `timeSeriesCountToGrid`, the ClickHouse counterpart of PromQL's `count_over_time`.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
struct AggregateFunctionTimeseriesCountTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    using ResultType = ValueType;

    static String getName() { return "timeSeriesCountToGrid"; }

    /// A count is exact under subtraction, so this summary is invertible and the window is an O(1) running sum.
    struct Summary
    {
        UInt64 count = 0;

        void add(TimestampType /*timestamp*/, ValueType /*value*/)
        {
            ++count;
        }

        void merge(const Summary & added)
        {
            count += added.count;
        }

        void unmerge(const Summary & leaving, const Summary * /*new_first*/)
        {
            count -= leaving.count;
        }
    };

    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;

        static_assert(decltype(sliding_sum)::is_invertible);

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            Summary summary;
            samples.forEachSample([&summary](TimestampType timestamp, ValueType value)
            {
                summary.add(timestamp, value);
            });

            if (summary.count == 0)
                return;
            sliding_sum.add(std::move(summary), bucket_end_timestamp);
        }

        void removeBefore(TimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<ResultType> getResult(TimestampType /*grid_timestamp*/) const
        {
            const Summary combined = sliding_sum.getCurrentSum();

            /// No samples in the window: Prometheus produces no output point for this grid timestamp.
            if (combined.count == 0)
                return std::nullopt;

            return static_cast<ResultType>(combined.count);
        }
    };

    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 1;
};


/// Counts time series values on a grid; the counterpart of PromQL's `count_over_time`.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesCount final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesCount<TimestampType_, IntervalType_, ValueType_>,
        AggregateFunctionTimeseriesCountTraits<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesCountTraits<TimestampType_, IntervalType_, ValueType_>;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesCount, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return {};
    }
};

}
