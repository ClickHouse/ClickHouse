#pragma once

#include <cmath>
#include <cstddef>
#include <optional>


#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

/// `is_stddev_` selects whether the result is the standard deviation (sqrt of the variance) or the variance itself.
template <typename TimestampType_, typename ValueType_, bool is_stddev_>
struct AggregateFunctionTimeseriesVarianceTraits
{
    static constexpr bool is_stddev = is_stddev_;

    using GridScaleTimestampType = DateTime64;
    using ValueType = ValueType_;
    using TimestampType = TimestampType_;
    /// The result is always Float64 regardless of the value type, like `varPop`/`stddevPop`.
    using ResultType = Float64;

    static String getName()
    {
        return is_stddev ? "timeSeriesStddevToGrid" : "timeSeriesStdvarToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Welford/Chan centered moments, as in `varPopStable`: no cancellation like in the naive `sum2 - sum * sum / n`
    /// formula, and `merge` is commutative and associative up to rounding. There is no `unmerge` because subtracting
    /// a bucket back out would lose precision, so the sliding sum uses its two-stacks or recompute strategy.
    struct Summary
    {
        UInt64 count = 0;
        Float64 mean = 0;  /// running mean
        Float64 m2 = 0;    /// sum of (x - mean)^2

        void add(ValueType value)
        {
            const Float64 x = static_cast<Float64>(value);
            ++count;
            const Float64 delta = x - mean;
            mean += delta / static_cast<Float64>(count);
            /// The trailing factor uses the just-updated `mean` (Welford).
            m2 += delta * (x - mean);
        }

        /// Chan et al.'s parallel merge of two centered-moment aggregates.
        void merge(const Summary & other)
        {
            if (other.count == 0)
                return;

            const Float64 na = static_cast<Float64>(count);
            const Float64 nb = static_cast<Float64>(other.count);
            const Float64 total = na + nb;
            const Float64 delta = other.mean - mean;

            mean += delta * nb / total;
            m2 += other.m2 + delta * delta * na * nb / total;
            count += other.count;
        }
    };

    /// Sliding aggregator: preaggregates each bucket into `{count, mean, m2}`, keeps the running combine over the
    /// window in a `SlidingSum` (two-stacks or recompute, chosen by the base from the thresholds below) and derives
    /// the (population) variance or standard deviation from it at each grid point.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<Summary> sliding_sum;

        explicit Aggregator(size_t stack_size) : sliding_sum(stack_size)
        {
        }

        void add(const Samples & samples, GridScaleTimestampType bucket_end_timestamp)
        {
            Summary summary;
            samples.forEachSample([&summary](TimestampType, ValueType value)
            {
                summary.add(value);
            });
            add(std::move(summary), bucket_end_timestamp);
        }

        void add(Summary summary, GridScaleTimestampType bucket_end_timestamp)
        {
            if (summary.count == 0)
                return;
            sliding_sum.add(std::move(summary), bucket_end_timestamp);
        }

        void removeBefore(GridScaleTimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<Float64> getResult(GridScaleTimestampType /*grid_timestamp*/) const
        {
            const Summary combined = sliding_sum.getCurrentSum();
            if (combined.count == 0)
                return std::nullopt;

            /// `m2` is a sum of non-negative terms, so the variance is never negative even with rounding.
            const Float64 variance = combined.m2 / static_cast<Float64>(combined.count);

            if constexpr (is_stddev)
                return std::sqrt(variance);
            else
                return variance;
        }
    };

    /// The bucket stores raw samples; the aggregator's `add(const Samples &)` preaggregates them into a `Summary`,
    /// so samples sharing a timestamp collapse to one before they contribute to the moments.
    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 1;

    /// Two-stacks thresholds, measured by the `timeseries_to_grid_two_stack_vs_recompute` example:
    /// two-stacks first wins at 4 buckets per window and is 2x faster from 10.
    static constexpr size_t AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS = 4;
    static constexpr size_t BPW_TO_FORCE_TWO_STACKS = 10;
};


/// Aggregate function to calculate PromQL-like stddev_over_time/stdvar_over_time (population standard
/// deviation/variance) of timeseries on the specified grid.
template <typename TimestampType_, typename ValueType_, bool is_stddev_>
class AggregateFunctionTimeseriesVariance final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesVariance<TimestampType_, ValueType_, is_stddev_>,
        AggregateFunctionTimeseriesVarianceTraits<TimestampType_, ValueType_, is_stddev_>>
{
public:
    using Traits = AggregateFunctionTimeseriesVarianceTraits<TimestampType_, ValueType_, is_stddev_>;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesVariance, Traits>;
    using Base::Base;

    typename Traits::Aggregator createAggregator(size_t stack_size_for_two_stacks) const
    {
        return typename Traits::Aggregator{stack_size_for_two_stacks};
    }
};

/// Each SQL function as a template with its `is_stddev` variant baked in, so registration names the
/// function directly.
template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesStddevToGrid = AggregateFunctionTimeseriesVariance<TimestampType, ValueType, /* is_stddev = */ true>;

template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesStdvarToGrid = AggregateFunctionTimeseriesVariance<TimestampType, ValueType, /* is_stddev = */ false>;

}
