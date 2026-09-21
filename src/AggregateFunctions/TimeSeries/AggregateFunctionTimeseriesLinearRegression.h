#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <limits>
#include <optional>


#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_predict_>
struct AggregateFunctionTimeseriesLinearRegressionTraits
{
    static constexpr bool is_predict = is_predict_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return is_predict ? "timeSeriesPredictLinearToGrid" : "timeSeriesDerivToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Per-bucket regression data, kept as numerically stable centered moments (Welford's algorithm with Chan's
    /// parallel merge). `mean_x`/`mean_y` are the running means; `m2_x = sum of (x - mean_x)^2` and
    /// `c_xy = sum of (x - mean_x)(y - mean_y)` are the centered (co)moments. Because the moments accumulate
    /// deviations from the running mean, they stay small (~`window^2`) and precise regardless of how far the
    /// window sits from `base`. The merge is order-independent, so buckets can be combined in any order.
    struct Summary
    {
        Float64 mean_x = 0;     /// running mean of x
        Float64 mean_y = 0;     /// running mean of y
        Float64 m2_x = 0;       /// sum of (x - mean_x)^2
        Float64 c_xy = 0;       /// sum of (x - mean_x)(y - mean_y)
        Float64 count = 0;      /// number of samples

        /// The samples' timestamps are centered on a common base (the grid start) before accumulating,
        /// so x stays small. The centering is necessary because otherwise a raw `DateTime64(9)` timestamp (~1.7e18)
        /// would exceed the Float64 mantissa, so distinct timestamps collapse to the same x.
        void add(TimestampType timestamp, ValueType value, TimestampType base)
        {
            const Float64 x = static_cast<Float64>(
                static_cast<Int128>(static_cast<Int64>(timestamp)) - static_cast<Int128>(static_cast<Int64>(base)));
            const Float64 y = static_cast<Float64>(value);

            ++count;
            const Float64 dx = x - mean_x;
            mean_x += dx / count;
            mean_y += (y - mean_y) / count;
            /// `dx` uses the old `mean_x`; the trailing factors use the just-updated means (Welford).
            m2_x += dx * (x - mean_x);
            c_xy += dx * (y - mean_y);
        }

        /// Chan's parallel merge of two centered-moment aggregates.
        void merge(const Summary & other)
        {
            if (other.count == 0)
                return;

            const Float64 na = count;
            const Float64 nb = other.count;
            const Float64 total = na + nb;
            const Float64 dx = other.mean_x - mean_x;
            const Float64 dy = other.mean_y - mean_y;

            mean_x += dx * nb / total;
            mean_y += dy * nb / total;
            m2_x += other.m2_x + dx * dx * na * nb / total;
            c_xy += other.c_xy + dx * dy * na * nb / total;
            count += other.count;
        }
    };

    /// Sliding aggregator for linear regression: preaggregates each bucket into centered moments, then keeps the
    /// running combine over the window in a `SlidingSum` (two-stacks or recompute, chosen by `createAggregator`).
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;
        TimestampType base;
        Float64 predict_offset;

        Aggregator(size_t stack_size, TimestampType base_, Float64 predict_offset_)
            : sliding_sum(stack_size), base(base_), predict_offset(predict_offset_)
        {
        }

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            /// Preaggregate the bucket's samples into centered moments; the merge is order-independent, so no sorting.
            Summary summary;
            samples.forEachSample([&summary, this](TimestampType timestamp, ValueType value)
            {
                summary.add(timestamp, value, base);
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

        std::optional<ValueType> getResult(TimestampType grid_timestamp) const
        {
            const Summary combined = sliding_sum.getCurrentSum();
            if (combined.count < 2 || combined.m2_x == 0)
                return std::nullopt;

            const Float64 slope = combined.c_xy / combined.m2_x;
            if (!is_predict)
                return static_cast<ValueType>(slope);

            /// Line y = slope * x + intercept with x centered on `base`; extrapolate to `grid_timestamp +
            /// predict_offset`, expressed in the same centered coordinates (subtract `base` in `Int128`).
            const Float64 intercept = combined.mean_y - slope * combined.mean_x;
            const Float64 predict_x = static_cast<Float64>(
                static_cast<Int128>(static_cast<Int64>(grid_timestamp)) - static_cast<Int128>(static_cast<Int64>(base)))
                + predict_offset;
            const Float64 predicted = slope * predict_x + intercept;
            return static_cast<ValueType>(predicted);
        }
    };

    /// The bucket stores raw samples; the aggregator's `add(const Samples &)` preaggregates them into a `Summary`.
    using Bucket = Samples;
};


template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_predict_>
class AggregateFunctionTimeseriesLinearRegression final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesLinearRegression<TimestampType_, IntervalType_, ValueType_, is_predict_>,
        AggregateFunctionTimeseriesLinearRegressionTraits<TimestampType_, IntervalType_, ValueType_, is_predict_>>
{
public:
    using Traits = AggregateFunctionTimeseriesLinearRegressionTraits<TimestampType_, IntervalType_, ValueType_, is_predict_>;

    static constexpr bool is_predict = Traits::is_predict;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesLinearRegression, Traits>;
    using Base::Base;

    /// Constructor for timeSeriesPredictLinearToGrid (is_predict = true).
    /// For timeSeriesDerivToGrid (is_predict = false) it reaches the base constructor via `using Base::Base` above.
    /// The base constructor takes the same arguments except predict_offset_.
    explicit AggregateFunctionTimeseriesLinearRegression(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_, UInt32 timestamp_scale_, Float64 predict_offset_)
        : Base(argument_types_, parameters_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , predict_offset(predict_offset_)
    {
    }

    /// `createAggregator` switches to the two-stack queue once the average number of populated buckets in a
    /// window reaches this value; below it, recomputing the window each grid point is cheaper. The
    /// `timeseries_to_grid_two_stack_vs_recompute` example measures the crossover by driving the real finalize over
    /// a larger-than-cache dataset (so recompute pays the same per-point cache misses as the real query) and puts
    /// it around 8-10 populated buckets per window, matching an end-to-end A/B. Sparse data needs no margin here:
    /// the density factor in `createAggregator` already converts `buckets_per_window` to the populated average.
    static constexpr size_t AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS = 10;

    /// Hard cap: regardless of average density, use two-stacks once a window can hold this many buckets. The
    /// density estimate below is an average, but density is not uniform - a low average can still hide a locally
    /// dense window whose recompute folds far more buckets than the average. Beyond this capacity we stop trusting
    /// the average and bound the worst case: at this size a fully dense window already makes recompute ~2x slower
    /// than two-stacks (measured by the `timeseries_to_grid_two_stack_vs_recompute` example).
    static constexpr size_t BPW_TO_FORCE_TWO_STACKS = 20;

    Aggregator createAggregator(size_t num_populated_buckets) const
    {
        /// Recompute folds the populated buckets in each window - on average `buckets_per_window * density`, where
        /// `density = num_populated_buckets / bucket_count`. Compare that average (not the dense maximum
        /// `buckets_per_window`) to the threshold, so sparse data, whose windows hold fewer populated buckets,
        /// stays on the cheaper recompute path without inflating the threshold. The hard cap still forces
        /// two-stacks for large windows, where a non-uniform spread could hide a locally dense window.
        const size_t avg_buckets_in_window = Base::bucket_count
            ? static_cast<size_t>(static_cast<double>(Base::buckets_per_window) * static_cast<double>(num_populated_buckets)
                / static_cast<double>(Base::bucket_count))
            : 0;
        const bool use_two_stacks = avg_buckets_in_window >= AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS
            || Base::buckets_per_window >= BPW_TO_FORCE_TWO_STACKS;
        /// Reserve at most `buckets_per_window`, but capped by `num_populated_buckets` - else a huge window
        /// (forced onto two-stacks by the hard cap) would `reserve(~INT64_MAX)` and fail to allocate.
        const size_t stack_size = use_two_stacks ? std::min(Base::buckets_per_window, num_populated_buckets) : 0;
        return Aggregator{stack_size, Base::start_timestamp, predict_offset};
    }

    static constexpr UInt16 FORMAT_VERSION = 2;
    static constexpr bool DateTime64Supported = true;

protected:
    const Float64 predict_offset{};    /// Predict offset used by timeSeriesPredictLinearToGrid function, used to calculate the timestamp of the predicted value
};

/// Each SQL function as a 3-argument template with its is_predict variant baked in, so registration names the
/// function directly.
template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesDerivToGrid = AggregateFunctionTimeseriesLinearRegression<TimestampType, IntervalType, ValueType, false>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesPredictLinearToGrid = AggregateFunctionTimeseriesLinearRegression<TimestampType, IntervalType, ValueType, true>;

}
