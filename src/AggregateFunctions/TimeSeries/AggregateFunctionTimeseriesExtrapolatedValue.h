#pragma once

#include <cstddef>
#include <cstring>


#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesDecimal.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>

#include <optional>

namespace DB
{

/// `is_rate` divides the accumulated value by the window;
/// `check_resets` counts resets and clamps extrapolation at zero.
template <typename TimestampType_, typename ValueType_, bool is_rate_, bool check_resets_>
struct AggregateFunctionTimeseriesExtrapolatedValueTraits
{
    static constexpr bool is_rate = is_rate_;
    static constexpr bool check_resets = check_resets_;
    using GridScaleTimestampType = DateTime64;
    using GridScaleIntervalType = Decimal64;
    using ValueType = ValueType_;
    using TimestampType = TimestampType_;
    using ResultType = Float64;

    static String getName()
    {
        if constexpr (is_rate)
            return "timeSeriesRateToGrid";
        else if constexpr (check_resets)
            return "timeSeriesIncreaseToGrid";
        else
            return "timeSeriesDeltaToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Summary of a bucket or of the whole window: first/last sample, count and reset adjustment.
    /// The timestamps keep the type of the input columns, they are converted to the grid in `getResult`.
    struct Summary
    {
        TimestampType first_timestamp = 0;
        ValueType first_value = 0;
        TimestampType last_timestamp = 0;
        ValueType last_value = 0;
        UInt64 count = 0;
        Float64 resets = 0;

        void merge(const Summary & added)
        {
            if (added.count == 0)
                return;
            if (count == 0)
            {
                *this = added;
                return;
            }
            if constexpr (check_resets)
            {
                if (last_value > added.first_value)
                    resets += static_cast<Float64>(last_value);     /// reset across the bucket boundary
            }
            resets += added.resets;
            last_timestamp = added.last_timestamp;
            last_value = added.last_value;
            count += added.count;
        }

        void unmerge(const Summary & leaving, const Summary * new_first)
        {
            resets -= leaving.resets;
            count -= leaving.count;
            if (new_first)
            {
                if constexpr (check_resets)
                {
                    if (leaving.last_value > new_first->first_value)
                        resets -= static_cast<Float64>(leaving.last_value);     /// drop the cross-boundary reset
                }
                first_timestamp = new_first->first_timestamp;
                first_value = new_first->first_value;
            }
        }
    };

    /// Sliding aggregator for rate/increase/delta: preaggregates each bucket into a `Summary` and keeps the window's
    /// summary in a `SlidingSum`. `Summary` is invertible, so the window is maintained with a single running
    /// sum in O(1) per bucket; `getResult` reads its first/last sample, count and resets.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<Summary> sliding_sum;

        /// `Summary::merge` is order-dependent (not commutative), so it must take the invertible running-sum path,
        /// not the two-stacks path which combines values out of time order.
        static_assert(decltype(sliding_sum)::is_invertible);

        GridScaleIntervalType window;
        Int64 grid_ticks_per_second;
        Int64 column_to_grid_multiplier;
        bool exact_rate;

        Aggregator(GridScaleIntervalType window_, Int64 grid_ticks_per_second_, Int64 column_to_grid_multiplier_, bool exact_rate_)
            : window(window_), grid_ticks_per_second(grid_ticks_per_second_), column_to_grid_multiplier(column_to_grid_multiplier_)
            , exact_rate(exact_rate_)
        {
        }

        void add(const Samples & samples, GridScaleTimestampType bucket_end_timestamp)
        {
            Summary summary;
            samples.forEachSample([&summary](TimestampType timestamp, ValueType value)
            {
                if (summary.count == 0)
                {
                    summary.first_timestamp = timestamp;
                    summary.first_value = value;
                }
                else if constexpr (check_resets)
                {
                    if (summary.last_value > value)
                        summary.resets += static_cast<Float64>(summary.last_value);
                }
                summary.last_timestamp = timestamp;
                summary.last_value = value;
                ++summary.count;
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
            const Summary combined = sliding_sum.getCurrentSum();

            if (exact_rate)
                return getExactResult(combined, grid_timestamp);

            /// Need at least two samples to calculate the rate or delta.
            if (combined.count < 2)
                return std::nullopt;

            const Int64 first_timestamp = static_cast<Int64>(combined.first_timestamp);
            const Float64 first_value = static_cast<Float64>(combined.first_value);
            const Int64 last_timestamp = static_cast<Int64>(combined.last_timestamp);
            const Float64 last_value = static_cast<Float64>(combined.last_value);
            const UInt64 total_count = combined.count;
            const Float64 total_resets = combined.resets;

            /// The extrapolation logic is copied from Prometheus' rate calculation
            /// (https://github.com/prometheus/prometheus/blob/5e124cf4f2b9467e4ae1c679840005e727efd599/promql/functions.go#L127),
            /// licensed under the Apache License 2.0.

            /// The timestamps of the samples have the scale of the input columns, the calculations below use the scale of the grid.
            /// The difference of two timestamps in a window fits Int64 at the scale of the grid because the window does.
            const GridScaleIntervalType time_difference = (last_timestamp - first_timestamp) * column_to_grid_multiplier;
            if (time_difference == 0)
                return std::nullopt;

            Float64 value_difference = last_value - first_value + total_resets;

            // Duration between first/last samples and boundary of range. Subtract in `Int128` first to avoid
            // both signed overflow on `grid_timestamp - window` and `Float64` precision loss when timestamps
            // are large (e.g. `DateTime64(9)` near present-day epoch ~1.7e18).
            Float64 duration_to_start = static_cast<Float64>(
                static_cast<Int128>(first_timestamp) * column_to_grid_multiplier
                - static_cast<Int128>(static_cast<Int64>(grid_timestamp))
                + static_cast<Int128>(static_cast<Int64>(window)));
            Float64 duration_to_end = static_cast<Float64>(
                static_cast<Int128>(static_cast<Int64>(grid_timestamp))
                - static_cast<Int128>(last_timestamp) * column_to_grid_multiplier);

            const Float64 average_duration_between_samples = static_cast<Float64>(time_difference) / static_cast<Float64>(total_count - 1);

            // If samples are close enough to the (lower or upper) boundary of the range, we extrapolate the
            // rate all the way to the boundary in question. "Close enough" is up to 10% more than the average
            // duration between samples within the range; otherwise we extrapolate by only half of the average
            // duration between samples (our guess for where the series actually starts or ends).
            const auto extrapolation_threshold = average_duration_between_samples * 1.1;
            Float64 extrapolate_to_interval = static_cast<Float64>(time_difference);

            if (duration_to_start >= extrapolation_threshold)
                duration_to_start = average_duration_between_samples / 2;

            if (check_resets && value_difference > 0 && first_value >= 0)
            {
                // Counters cannot be negative. If we have any slope at all we can extrapolate the zero point
                // of the counter; if that is closer than duration_to_start, take it as the start, avoiding
                // extrapolation to negative counter values.
                Float64 duration_to_zero = static_cast<Float64>(time_difference) * (first_value / value_difference);
                duration_to_start = std::min(duration_to_zero, duration_to_start);
            }

            extrapolate_to_interval += duration_to_start;

            if (duration_to_end >= extrapolation_threshold)
                duration_to_end = average_duration_between_samples / 2;
            extrapolate_to_interval += duration_to_end;

            Float64 factor = extrapolate_to_interval / static_cast<Float64>(time_difference);

            if constexpr (is_rate)
                factor = factor * static_cast<Float64>(grid_ticks_per_second) / static_cast<Float64>(window);

            value_difference *= factor;

            return value_difference;
        }

        /// The exact mode: no extrapolation to the boundaries of the window. The difference is measured from the last sample
        /// before the window if it is not older than `window` before the window's start, otherwise from the first sample in the window.
        std::optional<ResultType> getExactResult(const Summary & combined, GridScaleTimestampType grid_timestamp) const
        {
            if (combined.count == 0)
                return std::nullopt;

            const Float64 first_value = static_cast<Float64>(combined.first_value);
            const Float64 last_value = static_cast<Float64>(combined.last_value);
            Float64 value_difference = last_value - first_value + combined.resets;

            bool has_previous_sample = false;
            if (const auto & last_removed = sliding_sum.getLastRemoved())
            {
                const Summary & previous = last_removed->second;
                /// Subtract in `Int128` for the same reason as in `getResult`.
                const Int128 previous_timestamp = static_cast<Int128>(static_cast<Int64>(previous.last_timestamp)) * column_to_grid_multiplier;
                const Int128 window_length = static_cast<Int128>(static_cast<Int64>(window));
                const Int128 window_start = static_cast<Int128>(static_cast<Int64>(grid_timestamp)) - window_length;
                if (previous_timestamp <= window_start && window_start - previous_timestamp <= window_length)
                {
                    has_previous_sample = true;
                    const Float64 previous_value = static_cast<Float64>(previous.last_value);
                    value_difference += first_value - previous_value;
                    if (check_resets && previous_value > first_value)
                        value_difference += previous_value;     /// reset between the previous sample and the window
                }
            }

            /// Need at least two samples to calculate the rate or delta.
            if (!has_previous_sample && combined.count < 2)
                return std::nullopt;

            if constexpr (is_rate)
                value_difference = value_difference * static_cast<Float64>(grid_ticks_per_second) / static_cast<Float64>(window);

            return value_difference;
        }
    };

    /// The bucket stores raw samples; the aggregator's `add(const Samples &)` preaggregates them into a `Summary`.
    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 5;
};


/// Aggregate function to calculate extrapolated values (rate, increase and delta) of timeseries on the specified grid
template <typename TimestampType_, typename ValueType_, bool is_rate_, bool check_resets_>
class AggregateFunctionTimeseriesExtrapolatedValue final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesExtrapolatedValue<TimestampType_, ValueType_, is_rate_, check_resets_>,
        AggregateFunctionTimeseriesExtrapolatedValueTraits<TimestampType_, ValueType_, is_rate_, check_resets_>>
{
public:
    using Traits = AggregateFunctionTimeseriesExtrapolatedValueTraits<TimestampType_, ValueType_, is_rate_, check_resets_>;

    static constexpr bool is_rate = Traits::is_rate;
    static constexpr bool check_resets = Traits::check_resets;
    using GridScaleTimestampType = typename Traits::GridScaleTimestampType;
    using ValueType = typename Traits::ValueType;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesExtrapolatedValue, Traits>;

    /// `exact_rate` comes from the optional fifth parameter, so it is a part of the function's type.
    /// It only changes how the result is calculated, the state is the same in both modes.
    AggregateFunctionTimeseriesExtrapolatedValue(const DataTypes & argument_types_, const Array & parameters_,
        GridScaleTimestampType grid_start_, GridScaleTimestampType grid_end_, typename Traits::GridScaleIntervalType grid_step_,
        typename Traits::GridScaleIntervalType window_, UInt32 grid_scale_, UInt32 column_timestamp_scale_, bool exact_rate_)
        : Base(argument_types_, parameters_, grid_start_, grid_end_, grid_step_, window_, grid_scale_, column_timestamp_scale_)
        , exact_rate(exact_rate_)
    {
    }

    Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return Aggregator{Base::window, Base::grid_ticks_per_second, Base::column_to_grid_multiplier, exact_rate};
    }

private:
    const bool exact_rate;
};

/// Each SQL function as a template with its `is_rate` / `check_resets` variant baked in, so
/// registration names the function directly.
template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesRateToGrid = AggregateFunctionTimeseriesExtrapolatedValue<TimestampType, ValueType, /* is_rate = */ true, /* check_resets = */ true>;

template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesIncreaseToGrid = AggregateFunctionTimeseriesExtrapolatedValue<TimestampType, ValueType, /* is_rate = */ false, /* check_resets = */ true>;

template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesDeltaToGrid = AggregateFunctionTimeseriesExtrapolatedValue<TimestampType, ValueType, /* is_rate = */ false, /* check_resets = */ false>;

}
