#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>

#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

/// What the functions fitting a line to the samples in each window return for a grid point.
enum class TimeseriesLinearRegressionReturnKind : UInt8
{
    Slope,          /// `timeSeriesDerivToGrid`: the slope per second.
    Prediction,     /// `timeSeriesPredictLinearToGrid`: the value of the line at the grid point's timestamp plus a fixed `predict_offset`.
    InterceptAndSlope,   /// `timeSeriesLinearRegressionToGrid`: both the value of the line at the grid point's timestamp (`intercept`) and the
                    /// slope per second, so that the prediction for any offset is `intercept + slope * offset` (used by the PromQL
                    /// `predict_linear` with a per-step offset).
};

template <typename TimestampType_, typename ValueType_, TimeseriesLinearRegressionReturnKind return_kind_>
struct AggregateFunctionTimeseriesLinearRegressionTraits
{
    static constexpr TimeseriesLinearRegressionReturnKind return_kind = return_kind_;
    using GridScaleTimestampType = DateTime64;
    using TimestampType = TimestampType_;
    using ValueType = ValueType_;

    /// The results are calculated with double precision.
    using ResultType = std::conditional_t<return_kind == TimeseriesLinearRegressionReturnKind::InterceptAndSlope, std::pair<Float64, Float64>, Float64>;

    static String getName()
    {
        if constexpr (return_kind == TimeseriesLinearRegressionReturnKind::Slope)
            return "timeSeriesDerivToGrid";
        else if constexpr (return_kind == TimeseriesLinearRegressionReturnKind::Prediction)
            return "timeSeriesPredictLinearToGrid";
        else
            return "timeSeriesLinearRegressionToGrid";
    }

    static Strings getResultTupleElementNames() requires (return_kind == TimeseriesLinearRegressionReturnKind::InterceptAndSlope)
    {
        return {"intercept", "slope"};
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
        /// The timestamps keep the type of the input columns (`base` has the same type), the slope is converted to seconds
        /// in `Aggregator::getResult`. The subtraction is done in Int128 to be overflow-safe for any timestamps.
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
        AggregateFunctionTimeseriesSlidingSum<Summary> sliding_sum;

        /// The start of the grid converted to the type of the input columns: the samples are centered on it.
        /// Any fixed origin works for the centered moments, the conversion is taken into account in `getResult`.
        TimestampType base;
        Float64 predict_offset;         /// In seconds, can be non-finite.
        Int64 column_to_grid_multiplier;
        Int64 column_ticks_per_second;  /// Converts the slope per tick of the input columns to the slope per second.
        GridScaleTimestampType base_in_grid_scale;  /// `base` at the scale of the grid (`base * column_to_grid_multiplier`); fits in Int64 because `base` is `grid_start` divided by the multiplier.

        Aggregator(size_t stack_size, GridScaleTimestampType grid_start_, Float64 predict_offset_, Int64 column_to_grid_multiplier_, Int64 column_ticks_per_second_)
            : sliding_sum(stack_size), base(getBase(grid_start_, column_to_grid_multiplier_))
            , predict_offset(predict_offset_)
            , column_to_grid_multiplier(column_to_grid_multiplier_), column_ticks_per_second(column_ticks_per_second_)
            , base_in_grid_scale(static_cast<GridScaleTimestampType>(static_cast<Int64>(base) * column_to_grid_multiplier_))
        {
        }

        /// Converts the start of the grid to the type of the input columns, rounding towards zero.
        /// The grid can start outside the range of the type (for example, before 1970 for UInt32), then the nearest
        /// representable timestamp is used: the samples are in the range, so their distances to it stay exact in Float64.
        static TimestampType getBase(GridScaleTimestampType grid_start, Int64 column_to_grid_multiplier)
        {
            const Int64 base = static_cast<Int64>(grid_start) / column_to_grid_multiplier;
            if constexpr (std::is_integral_v<TimestampType>)
                return static_cast<TimestampType>(std::clamp<Int64>(base, std::numeric_limits<TimestampType>::min(), std::numeric_limits<TimestampType>::max()));
            else
                return TimestampType(base);
        }

        void add(const Samples & samples, GridScaleTimestampType bucket_end_timestamp)
        {
            /// Preaggregate the bucket's samples into centered moments; the accumulation is order-independent, so any iteration order would do.
            Summary summary;
            samples.forEachSample([&summary, this](TimestampType timestamp, ValueType value)
            {
                summary.add(timestamp, value, base);
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

        std::optional<ResultType> getResult(GridScaleTimestampType grid_timestamp) const
        {
            const Summary & combined = sliding_sum.getCurrentSum();
            if (combined.count < 2 || combined.m2_x == 0)
                return std::nullopt;

            /// The slope of the line per tick of the input columns, and per second.
            const Float64 slope_per_column_tick = combined.c_xy / combined.m2_x;
            const Float64 slope_per_second = slope_per_column_tick * static_cast<Float64>(column_ticks_per_second);

            if constexpr (return_kind == TimeseriesLinearRegressionReturnKind::Slope)
            {
                return slope_per_second;
            }
            else
            {
                /// The grid point in the centered coordinates of the samples (ticks of the input columns counted from `base`):
                /// the distance is calculated exactly in Int128 at the scale of the grid to be overflow-safe, then converted.
                const Float64 grid_x = static_cast<Float64>(
                    static_cast<Int128>(static_cast<Int64>(grid_timestamp)) - static_cast<Int128>(static_cast<Int64>(base_in_grid_scale)))
                    / static_cast<Float64>(column_to_grid_multiplier);

                if constexpr (return_kind == TimeseriesLinearRegressionReturnKind::Prediction)
                {
                    /// Line y = slope_per_column_tick * x + intercept with x centered on `base`; extrapolate to `grid_timestamp + predict_offset`
                    /// expressed in the same centered coordinates. The offset is added in Float64, so Inf or NaN propagates to the result like in PromQL.
                    const Float64 intercept_at_base = combined.mean_y - slope_per_column_tick * combined.mean_x;
                    const Float64 predict_x = grid_x + predict_offset * static_cast<Float64>(column_ticks_per_second);
                    return slope_per_column_tick * predict_x + intercept_at_base;
                }
                else
                {
                    /// The returned intercept is the one with time counted from the grid point, i.e. the value of the line
                    /// there. This is how Prometheus's linearRegression() defines the intercept it returns.
                    const Float64 intercept_at_grid_point = combined.mean_y + slope_per_column_tick * (grid_x - combined.mean_x);
                    return ResultType{intercept_at_grid_point, slope_per_second};
                }
            }
        }
    };

    /// The bucket stores raw samples; the aggregator's `add(const Samples &)` preaggregates them into a `Summary`.
    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 4;

    /// `getStackSizeForTwoStacks` switches to the two-stack queue once the average number of populated buckets
    /// in a window reaches this value; below it, recomputing the window each grid point is cheaper. The
    /// `timeseries_to_grid_two_stack_vs_recompute` example measures the crossover by driving the real finalize over
    /// a larger-than-cache dataset (so recompute pays the same per-point cache misses as the real query) and puts
    /// it at 4 populated buckets per window. Sparse data needs no margin here: the density factor in
    /// `getStackSizeForTwoStacks` already converts `buckets_per_window` to the populated average.
    static constexpr size_t AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS = 4;

    /// Hard cap: regardless of average density, use two-stacks once a window can hold this many buckets. The
    /// density estimate in `getStackSizeForTwoStacks` is an average, but density is not uniform - a low average
    /// can still hide a locally dense window whose recompute folds far more buckets than the average. Beyond this
    /// capacity we stop trusting the average and bound the worst case: at this size a fully dense window already
    /// makes recompute ~2x slower than two-stacks (measured by the `timeseries_to_grid_two_stack_vs_recompute` example).
    static constexpr size_t BPW_TO_FORCE_TWO_STACKS = 12;
};


template <typename TimestampType_, typename ValueType_, TimeseriesLinearRegressionReturnKind return_kind_>
class AggregateFunctionTimeseriesLinearRegression final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesLinearRegression<TimestampType_, ValueType_, return_kind_>,
        AggregateFunctionTimeseriesLinearRegressionTraits<TimestampType_, ValueType_, return_kind_>>
{
public:
    using Traits = AggregateFunctionTimeseriesLinearRegressionTraits<TimestampType_, ValueType_, return_kind_>;

    static constexpr TimeseriesLinearRegressionReturnKind return_kind = Traits::return_kind;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesLinearRegression, Traits>;
    using Base::Base;
    using GridScaleTimestampType = typename Base::GridScaleTimestampType;
    using GridScaleIntervalType = typename Base::GridScaleIntervalType;
    using ValueType = typename Traits::ValueType;
    using Aggregator = typename Traits::Aggregator;

    /// Constructor for timeSeriesPredictLinearToGrid (return_kind = Prediction).
    /// The other functions reach the base constructor via `using Base::Base` above,
    /// it takes the same arguments except predict_offset_.
    explicit AggregateFunctionTimeseriesLinearRegression(const DataTypes & argument_types_, const Array & parameters_,
        GridScaleTimestampType grid_start_, GridScaleTimestampType grid_end_, GridScaleIntervalType grid_step_, GridScaleIntervalType window_, UInt32 grid_scale_,
        UInt32 column_timestamp_scale_, Float64 predict_offset_)
        : Base(argument_types_, parameters_, grid_start_, grid_end_, grid_step_, window_, grid_scale_, column_timestamp_scale_)
        , predict_offset(predict_offset_)
    {
    }

    Aggregator createAggregator(size_t stack_size_for_two_stacks) const
    {
        return Aggregator{stack_size_for_two_stacks, Base::grid_start, predict_offset, Base::column_to_grid_multiplier, Base::column_ticks_per_second};
    }

protected:
    const Float64 predict_offset{};    /// Predict offset in seconds used by timeSeriesPredictLinearToGrid function, used to calculate the timestamp of the predicted value
};

/// Each SQL function as a template with its variant baked in, so registration names the function directly.
template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesDerivToGrid = AggregateFunctionTimeseriesLinearRegression<TimestampType, ValueType, TimeseriesLinearRegressionReturnKind::Slope>;

template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesPredictLinearToGrid = AggregateFunctionTimeseriesLinearRegression<TimestampType, ValueType, TimeseriesLinearRegressionReturnKind::Prediction>;

template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesLinearRegressionToGrid = AggregateFunctionTimeseriesLinearRegression<TimestampType, ValueType, TimeseriesLinearRegressionReturnKind::InterceptAndSlope>;

}
