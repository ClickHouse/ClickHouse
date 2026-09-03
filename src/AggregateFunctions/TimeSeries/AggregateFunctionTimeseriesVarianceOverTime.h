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
template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_stddev_>
struct AggregateFunctionTimeseriesVarianceOverTimeTraits
{
    static constexpr bool is_stddev = is_stddev_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;
    /// Population variance/stddev are documented as Float64 regardless of the stored value type,
    /// matching the ordinary varPop/stddevPop family.
    using ResultValueType = Float64;

    static String getName()
    {
        return is_stddev ? "timeSeriesStddevToGrid" : "timeSeriesStdvarToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Running Welford/Chan `{count, mean, m2}` accumulator - the same technique as
    /// `AggregateFunctionTimeseriesLinearRegression::Summary` and ClickHouse's own `varPopStable`/`stddevPopStable`
    /// (`AggregateFunctionVarianceData` in `AggregateFunctionStatistics.cpp`). `mean` is the running mean and `m2`
    /// is the sum of squared deviations from it (`sum((x - mean)^2)`), updated incrementally per sample (Welford,
    /// `add`) and combined pairwise via Chan et al.'s parallel-merge formula (`merge`), which is commutative and
    /// associative up to floating-point rounding, so buckets can be preaggregated and combined in any order.
    ///
    /// This replaces an earlier raw `{count, sum, sum2}` accumulator finalized as `sum2 - sum * sum / count`: for
    /// real-world data (e.g. byte counters ~5e8) `sum` and `sum2` reach magnitudes (~1e9, ~1e17) far beyond what a
    /// tiny true variance needs precision for, so the subtraction of two nearly-equal ~1e17 quantities lost every
    /// significant digit and could silently round an exact positive variance all the way down to `0` (confirmed
    /// empirically: two samples `540000000`/`540000001` - true population variance `0.25` - came out as exactly
    /// `0`). Because `m2` accumulates deviations from the mean rather than raw sums of squares, it stays as small
    /// as the series' actual spread regardless of the values' magnitude, so this cancellation cannot happen.
    ///
    /// Deliberately *not* invertible (no `unmerge`): subtracting one `m2`/`mean` back out of a combined one is
    /// exposed to the same class of cancellation, so - like `AggregateFunctionTimeseriesLinearRegression` - this
    /// only ever combines by `merge`, leaving `AggregateFunctionTimeseriesSlidingSum` on the two-stacks/recompute
    /// strategies (which only ever combine values via `merge`, never subtract a running combine).
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

        /// Chan et al.'s parallel merge of two centered-moment aggregates (same formula as
        /// `AggregateFunctionTimeseriesLinearRegression::Summary::merge`).
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
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;

        explicit Aggregator(size_t stack_size) : sliding_sum(stack_size)
        {
        }

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            /// Preaggregate the bucket's samples; `forEachSample` visits them with duplicate timestamps already
            /// collapsed, so each timestamp contributes exactly one sample to the moments.
            Summary summary;
            samples.forEachSample([&summary](TimestampType, ValueType value)
            {
                summary.add(value);
            });
            add(std::move(summary), bucket_end_timestamp);
        }

        void add(Summary summary, TimestampType bucket_end_timestamp)
        {
            if (summary.count == 0)
                return;
            sliding_sum.add(std::move(summary), bucket_end_timestamp);
        }

        void removeBefore(TimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<Float64> getResult(TimestampType /*grid_timestamp*/) const
        {
            const Summary combined = sliding_sum.getCurrentSum();
            if (combined.count == 0)
                return std::nullopt;

            /// `combined.m2` is already the numerically stable sum of squared deviations from the mean over the
            /// whole window (Welford/Chan, see `Summary` above), so population variance is simply its average.
            /// Due to floating-point rounding the result can be slightly less than zero even though variance is
            /// mathematically non-negative, so a genuinely negative *finite* result is clamped to zero before an
            /// eventual sqrt. NaN/Inf (e.g. from a genuine non-finite user sample, which the Prometheus
            /// storage path stores raw and unfiltered) must NOT be clamped
            /// here: `std::max(0.0, NaN)` would return `0.0` (any comparison against NaN is false), silently
            /// hiding bad input as a clean zero-variance series, so non-finite results are left untouched and
            /// propagate through as NaN/Inf.
            Float64 variance = combined.m2 / static_cast<Float64>(combined.count);
            if (std::isfinite(variance) && variance < 0.0)
                variance = 0.0;

            if constexpr (is_stddev)
                return std::sqrt(variance);
            else
                return variance;
        }
    };

    /// The bucket stores raw samples; the aggregator's `add(const Samples &)` preaggregates them into a `Summary`.
    /// Accumulating the moments directly instead would count duplicate timestamps twice, breaking the family's
    /// shared rule that samples sharing a timestamp collapse to one (`timeseriesMaxValueForDuplicateTimestamp`).
    using Bucket = Samples;

    /// Bumped whenever the serialized bucket layout changes: raw `{sum, sum2}` (1) -> Welford/Chan `{mean, m2}`
    /// (2) -> raw samples (3), so states written by an older peer are rejected rather than misread.
    static constexpr UInt16 FORMAT_VERSION = 3;

    /// `AggregateFunctionTimeseriesBase::getStackSizeForTwoStacks` switches to the two-stack queue once the
    /// average number of populated buckets in a window reaches this value; below it, recomputing the window
    /// each grid point is cheaper. Mirrors the threshold tuned for
    /// `AggregateFunctionTimeseriesLinearRegression` (see its `timeseries_to_grid_two_stack_vs_recompute`
    /// derivation) - our `Summary::merge` is exactly as cheap as regression's, so the same crossover applies.
    static constexpr size_t AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS = 10;

    /// Hard cap: regardless of average density, use two-stacks once a window can hold this many buckets (see
    /// `AggregateFunctionTimeseriesLinearRegression` for the full rationale).
    static constexpr size_t BPW_TO_FORCE_TWO_STACKS = 20;
};


/// Aggregate function to calculate PromQL-like stddev_over_time/stdvar_over_time (population standard
/// deviation/variance) of timeseries on the specified grid.
template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_stddev_>
class AggregateFunctionTimeseriesVarianceOverTime final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesVarianceOverTime<TimestampType_, IntervalType_, ValueType_, is_stddev_>,
        AggregateFunctionTimeseriesVarianceOverTimeTraits<TimestampType_, IntervalType_, ValueType_, is_stddev_>>
{
public:
    using Traits = AggregateFunctionTimeseriesVarianceOverTimeTraits<TimestampType_, IntervalType_, ValueType_, is_stddev_>;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesVarianceOverTime, Traits>;
    using Base::Base;

    typename Traits::Aggregator createAggregator(size_t stack_size_for_two_stacks) const
    {
        return typename Traits::Aggregator{stack_size_for_two_stacks};
    }

    static constexpr bool DateTime64Supported = true;
};

/// Each SQL function as a 3-argument template with its `is_stddev` variant baked in, so registration names the
/// function directly.
template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesStddevToGrid = AggregateFunctionTimeseriesVarianceOverTime<TimestampType, IntervalType, ValueType, /* is_stddev = */ true>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesStdvarToGrid = AggregateFunctionTimeseriesVarianceOverTime<TimestampType, IntervalType, ValueType, /* is_stddev = */ false>;

}
