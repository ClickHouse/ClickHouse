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
struct AggregateFunctionTimeseriesCountToGridTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType, /* keep_duplicates = */ true>;

    /// `Float64` rather than `ValueType`: a `Float32` column would round counts above 16777216.
    using ResultType = Float64;

    static String getName() { return "timeSeriesCountToGrid"; }

    /// A sample count is exact under subtraction as well as addition, so unlike sum and avg this summary is
    /// invertible and the window is kept as an O(1) running sum.
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
            /// `Samples` keeps duplicate timestamps, so every occurrence is counted.
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

    /// The bucket stores raw samples with duplicate timestamps kept, so every occurrence is counted.
    using Bucket = Samples;

    /// Bumped from 1: buckets now keep duplicate timestamps, and an old binary would silently re-collapse them.
    static constexpr UInt16 FORMAT_VERSION = 2;
};


/// Aggregate function to count timeseries values on the specified grid, i.e. the ClickHouse counterpart of
/// PromQL's `count_over_time`.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesCountToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesCountToGrid<TimestampType_, IntervalType_, ValueType_>,
        AggregateFunctionTimeseriesCountToGridTraits<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesCountToGridTraits<TimestampType_, IntervalType_, ValueType_>;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesCountToGrid, Traits>;
    using Base::Base;

    /// The summary is invertible, so the two-stacks queue is never used and its size is irrelevant here.
    Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return {};
    }
};

}
