#pragma once

#include <cmath>
#include <cstddef>
#include <optional>
#include <utility>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

/// Traits for `timeSeriesSumToGrid` and `timeSeriesAvgToGrid`, the ClickHouse counterparts of PromQL's
/// `sum_over_time` and `avg_over_time`. Both keep the same compensated sum per window (in the generic
/// `AggregateFunctionTimeseriesSlidingSum` container); avg divides it by the count.
template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_avg_>
struct AggregateFunctionTimeseriesCompensatedSumTraits
{
    static constexpr bool is_avg = is_avg_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    using ResultType = ValueType;

    static String getName()
    {
        return is_avg ? "timeSeriesAvgToGrid" : "timeSeriesSumToGrid";
    }

    /// Float64 sum with Kahan-Babuska-Neumaier compensation and a sample count. Without compensation a plain sum
    /// would swallow small samples next to a large one, and its rounding would depend on how the two-stacks queue
    /// groups the buckets. No `unmerge`: Float64 subtraction does not undo an addition (1e18 + 2 + 3 + 5 - 1e18 gives 0).
    struct Summary
    {
        Float64 sum = 0;
        Float64 compensation = 0;
        UInt64 count = 0;

        void add(TimestampType /*timestamp*/, ValueType value)
        {
            addCompensated(static_cast<Float64>(value));
            ++count;
        }

        void merge(const Summary & added)
        {
            addCompensated(added.sum);
            compensation = std::isfinite(sum) ? compensation + added.compensation : 0;
            count += added.count;
        }

        Float64 getSum() const { return sum + compensation; }

    private:
        void addCompensated(Float64 value)
        {
            const Float64 t = sum + value;
            /// A non-finite sum keeps no compensation: `Inf - Inf` is NaN, and Prometheus returns such a sum as is.
            if (!std::isfinite(t))
                compensation = 0;
            else if (std::abs(sum) >= std::abs(value))
                compensation += (sum - t) + value;
            else
                compensation += (value - t) + sum;
            sum = t;
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

            if constexpr (is_avg)
                return static_cast<ResultType>(combined.getSum() / static_cast<Float64>(combined.count));
            else
                return static_cast<ResultType>(combined.getSum());
        }
    };

    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 1;

    /// Two-stacks thresholds, measured by the `timeseries_to_grid_two_stack_vs_recompute` example:
    /// two-stacks first wins at 6 buckets per window and is 2x faster from 18.
    static constexpr size_t AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS = 6;
    static constexpr size_t BPW_TO_FORCE_TWO_STACKS = 18;
};


/// Calculates the sum (`is_avg_` = false) or the average (`is_avg_` = true) of time series values on a grid;
/// the counterparts of PromQL's `sum_over_time` and `avg_over_time`.
template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_avg_>
class AggregateFunctionTimeseriesCompensatedSum final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesCompensatedSum<TimestampType_, IntervalType_, ValueType_, is_avg_>,
        AggregateFunctionTimeseriesCompensatedSumTraits<TimestampType_, IntervalType_, ValueType_, is_avg_>>
{
public:
    using Traits = AggregateFunctionTimeseriesCompensatedSumTraits<TimestampType_, IntervalType_, ValueType_, is_avg_>;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesCompensatedSum, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t stack_size_for_two_stacks) const
    {
        return Aggregator{stack_size_for_two_stacks};
    }
};

/// Each SQL function as a 3-argument template with its is_avg variant baked in, so registration names the
/// function directly.
template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesSumToGrid = AggregateFunctionTimeseriesCompensatedSum<TimestampType, IntervalType, ValueType, /* is_avg_ = */ false>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesAvgToGrid = AggregateFunctionTimeseriesCompensatedSum<TimestampType, IntervalType, ValueType, /* is_avg_ = */ true>;

}
