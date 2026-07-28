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

    /// Running `{count, sum, sum2}` accumulator. `merge` is commutative and associative (plain field-wise addition),
    /// so buckets can accumulate their samples directly - there is no need to keep raw samples around for later
    /// preaggregation.
    ///
    /// Deliberately *not* invertible (no `unmerge`): `sum2` for real-world data (e.g. byte counters ~1e9) reaches
    /// ~1e17-1e18, where a merge-then-unmerge round trip (add a bucket, later subtract it back out) loses enough
    /// precision to leave a residual of tens to hundreds - enough to turn an exact 0 variance into a visibly wrong
    /// value. (Confirmed empirically: for two real samples ~5.4e8, `(v0^2 + v1^2) - v0^2` recovers `v1^2` off by
    /// -64.0, i.e. a spurious `variance=64`/`stddev=8` where the correct answer is exactly 0.) `AggregateFunctionTimeseriesLinearRegression`
    /// hits the same class of issue for the same reason and likewise avoids `unmerge` despite an order-independent
    /// merge. Omitting `unmerge` makes `AggregateFunctionTimeseriesSlidingSum` fall back to the two-stacks/recompute
    /// strategies, which only ever combine values by addition (never subtract a running sum), so this cancellation
    /// cannot happen.
    struct Summary
    {
        UInt64 count = 0;
        Float64 sum = 0;
        Float64 sum2 = 0;

        void add(TimestampType /*timestamp*/, ValueType value)
        {
            const Float64 v = static_cast<Float64>(value);
            ++count;
            sum += v;
            sum2 += v * v;
        }

        void merge(const Summary & other)
        {
            count += other.count;
            sum += other.sum;
            sum2 += other.sum2;
        }

        void serialize(WriteBuffer & buf) const
        {
            writeBinaryLittleEndian(count, buf);
            writeBinaryLittleEndian(sum, buf);
            writeBinaryLittleEndian(sum2, buf);
        }

        void deserialize(ReadBuffer & buf)
        {
            readBinaryLittleEndian(count, buf);
            readBinaryLittleEndian(sum, buf);
            readBinaryLittleEndian(sum2, buf);
        }

        /// Unlike e.g. `AggregateFunctionTimeseriesToGridSparseTraits::Summary`, this summary carries no timestamp
        /// to validate - required by the `Bucket` contract only.
        template <typename RangeType>
        void checkTimestampsInRange(const RangeType &) const
        {
        }
    };

    /// Sliding aggregator: keeps the running `{count, sum, sum2}` combine over the window in a `SlidingSum` (two-stacks
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

            const Float64 count = static_cast<Float64>(combined.count);

            /// Naive (non-Welford) population variance - the same formula ClickHouse's own `varPop`/`stddevPop`
            /// use by default (`VarMoments::getPopulation()`); Welford's algorithm is reserved for the explicitly
            /// named `*Stable` variants. Due to numerical errors the result can be slightly less than zero even
            /// though variance is mathematically non-negative, so it is clamped to zero before an eventual sqrt.
            const Float64 variance = std::max(0.0, (combined.sum2 - combined.sum * combined.sum / count) / count);

            if constexpr (is_stddev)
                return static_cast<ValueType>(std::sqrt(variance));
            else
                return static_cast<ValueType>(variance);
        }
    };

    /// No raw-sample preaggregation is needed (see `Summary` above) - the bucket accumulates `{count, sum, sum2}`
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

    static constexpr UInt16 FORMAT_VERSION = 1;
    static constexpr bool DateTime64Supported = true;
};

/// Each SQL function as a 3-argument template with its `is_stddev` variant baked in, so registration names the
/// function directly.
template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesStddevToGrid = AggregateFunctionTimeseriesVarianceOverTime<TimestampType, IntervalType, ValueType, /* is_stddev = */ true>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesStdvarToGrid = AggregateFunctionTimeseriesVarianceOverTime<TimestampType, IntervalType, ValueType, /* is_stddev = */ false>;

}
