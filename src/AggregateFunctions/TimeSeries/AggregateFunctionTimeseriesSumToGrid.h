#pragma once

#include <cstddef>
#include <optional>
#include <utility>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

/// Traits for `timeSeriesSumToGrid`, the ClickHouse counterpart of PromQL's `sum_over_time`.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
struct AggregateFunctionTimeseriesSumToGridTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType, /* keep_duplicates = */ true>;

    using ResultType = ValueType;

    static String getName() { return "timeSeriesSumToGrid"; }

    /// Deliberately has no `unmerge`: subtracting a departed value does not restore the sum it was folded into
    /// (`1e18 + 2 + 3 + 5 - 1e18` gives 0), so the window is recomputed instead of maintained invertibly.
    struct Summary
    {
        Float64 sum = 0;
        UInt64 count = 0;

        void add(TimestampType /*timestamp*/, ValueType value)
        {
            sum += static_cast<Float64>(value);
            ++count;
        }

        void merge(const Summary & added)
        {
            sum += added.sum;
            count += added.count;
        }
    };

    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;

        static_assert(!decltype(sliding_sum)::is_invertible);

        explicit Aggregator(size_t stack_size)
            : sliding_sum(stack_size)
        {
        }

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            /// `Samples` keeps duplicate timestamps, so every occurrence is folded into the sum.
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

            return static_cast<ResultType>(combined.sum);
        }
    };

    /// The bucket stores raw samples with duplicate timestamps kept, so every occurrence is summed.
    using Bucket = Samples;

    /// Bumped from 1: buckets now keep duplicate timestamps, and an old binary would silently re-collapse them.
    static constexpr UInt16 FORMAT_VERSION = 2;

    /// No two-stacks thresholds, so `getStackSizeForTwoStacks` never opts in and the window is always recomputed.
    /// The queue needs an *associative* merge to regroup summaries, and Float64 addition is not: for one-sample
    /// buckets [1], [1], [1e16] the recompute path folds (1 + 1) + 1e16 = 1e16 + 2, while a queue holding the
    /// oldest bucket in `front_stack` combines 1 + (1 + 1e16) = 1e16. Commutativity, which it does have, is not
    /// enough on its own.
};


/// Aggregate function to calculate the sum of timeseries values on the specified grid, i.e. the ClickHouse
/// counterpart of PromQL's `sum_over_time`.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesSumToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesSumToGrid<TimestampType_, IntervalType_, ValueType_>,
        AggregateFunctionTimeseriesSumToGridTraits<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesSumToGridTraits<TimestampType_, IntervalType_, ValueType_>;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesSumToGrid, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t stack_size_for_two_stacks) const
    {
        return Aggregator{stack_size_for_two_stacks};
    }
};

}
