#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <optional>


#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
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

    static String getName()
    {
        return is_stddev ? "timeSeriesStddevToGrid" : "timeSeriesStdvarToGrid";
    }

    /// Running Welford/Chan `{count, mean, m2}` accumulator - the same technique as
    /// `AggregateFunctionTimeseriesLinearRegression::Summary` and ClickHouse's own `varPopStable`/`stddevPopStable`
    /// (`AggregateFunctionVarianceData` in `AggregateFunctionStatistics.cpp`). `mean` is the running mean and `m2`
    /// is the sum of squared deviations from it (`sum((x - mean)^2)`), updated incrementally per sample (Welford,
    /// `add`) and combined pairwise via Chan et al.'s parallel-merge formula (`merge`), which is commutative and
    /// associative up to floating-point rounding, so buckets can accumulate their samples directly - there is no
    /// need to keep raw samples around for later preaggregation.
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

        void add(TimestampType /*timestamp*/, ValueType value)
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

        void serialize(WriteBuffer & buf) const
        {
            writeBinaryLittleEndian(count, buf);
            writeBinaryLittleEndian(mean, buf);
            writeBinaryLittleEndian(m2, buf);
        }

        void deserialize(ReadBuffer & buf)
        {
            readBinaryLittleEndian(count, buf);
            readBinaryLittleEndian(mean, buf);
            readBinaryLittleEndian(m2, buf);
        }

        /// Unlike e.g. `AggregateFunctionTimeseriesToGridSparseTraits::Summary`, this summary carries no timestamp
        /// to validate - required by the `Bucket` contract only.
        template <typename RangeType>
        void checkTimestampsInRange(const RangeType &) const
        {
        }
    };

    /// Sliding aggregator: keeps the running `{count, mean, m2}` combine over the window in a `SlidingSum` (two-stacks
    /// or recompute, chosen by `createAggregator`) and derives the (population) variance or standard deviation from
    /// it at each grid point.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;

        explicit Aggregator(size_t stack_size) : sliding_sum(stack_size)
        {
        }

        void add(const Summary & summary, TimestampType bucket_end_timestamp)
        {
            if (summary.count == 0)
                return;
            sliding_sum.add(Summary(summary), bucket_end_timestamp);
        }

        void removeBefore(TimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/) const
        {
            const Summary combined = sliding_sum.getCurrentSum();
            if (combined.count == 0)
                return std::nullopt;

            /// `combined.m2` is already the numerically stable sum of squared deviations from the mean over the
            /// whole window (Welford/Chan, see `Summary` above), so population variance is simply its average.
            /// Due to floating-point rounding the result can be slightly less than zero even though variance is
            /// mathematically non-negative, so a genuinely negative *finite* result is clamped to zero before an
            /// eventual sqrt. NaN/Inf (e.g. from a NaN/Inf sample in the window - the Prometheus storage path
            /// stores raw Float64 values unfiltered) must NOT be clamped here: `std::max(0.0, NaN)` would return
            /// `0.0` (any comparison against NaN is false), silently hiding bad input as a clean zero-variance
            /// series, so non-finite results are left untouched and propagate through as NaN/Inf.
            Float64 variance = combined.m2 / static_cast<Float64>(combined.count);
            if (std::isfinite(variance) && variance < 0.0)
                variance = 0.0;

            if constexpr (is_stddev)
                return static_cast<ValueType>(std::sqrt(variance));
            else
                return static_cast<ValueType>(variance);
        }
    };

    /// No raw-sample preaggregation is needed (see `Summary` above) - the bucket accumulates `{count, mean, m2}`
    /// directly as samples are added.
    using Bucket = Summary;
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

    /// `createAggregator` switches to the two-stack queue once the average number of populated buckets in a
    /// window reaches this value; below it, recomputing the window each grid point is cheaper. Mirrors the
    /// threshold tuned for `AggregateFunctionTimeseriesLinearRegression` (see its `timeseries_to_grid_two_stack_vs_recompute`
    /// derivation) - our `Summary::merge` is exactly as cheap as regression's, so the same crossover applies.
    static constexpr size_t AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS = 10;

    /// Hard cap: regardless of average density, use two-stacks once a window can hold this many buckets (see
    /// `AggregateFunctionTimeseriesLinearRegression` for the full rationale).
    static constexpr size_t BPW_TO_FORCE_TWO_STACKS = 20;

    typename Traits::Aggregator createAggregator(size_t num_populated_buckets) const
    {
        const size_t avg_buckets_in_window = Base::bucket_count
            ? static_cast<size_t>(static_cast<double>(Base::buckets_per_window) * static_cast<double>(num_populated_buckets)
                / static_cast<double>(Base::bucket_count))
            : 0;
        const bool use_two_stacks = avg_buckets_in_window >= AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS
            || Base::buckets_per_window >= BPW_TO_FORCE_TWO_STACKS;
        const size_t stack_size = use_two_stacks ? std::min(Base::buckets_per_window, num_populated_buckets) : 0;
        return typename Traits::Aggregator{stack_size};
    }

    /// Bumped from 1: `Summary`'s serialized layout changed meaning (raw `{sum, sum2}` -> Welford/Chan
    /// `{mean, m2}`), so old serialized states must not be misread as the new format.
    static constexpr UInt16 FORMAT_VERSION = 2;
    static constexpr bool DateTime64Supported = true;
};

/// Each SQL function as a 3-argument template with its `is_stddev` variant baked in, so registration names the
/// function directly.
template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesStddevToGrid = AggregateFunctionTimeseriesVarianceOverTime<TimestampType, IntervalType, ValueType, /* is_stddev = */ true>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesStdvarToGrid = AggregateFunctionTimeseriesVarianceOverTime<TimestampType, IntervalType, ValueType, /* is_stddev = */ false>;

}
