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
template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_rate_, bool check_resets_>
struct AggregateFunctionTimeseriesExtrapolatedValueTraits
{
    static constexpr bool is_rate = is_rate_;
    static constexpr bool check_resets = check_resets_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;
    using ResultType = ValueType_;

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
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;

        /// `Summary::merge` is order-dependent (not commutative), so it must take the invertible running-sum path,
        /// not the two-stacks path which combines values out of time order.
        static_assert(decltype(sliding_sum)::is_invertible);

        IntervalType window;
        TimestampType timestamp_scale_multiplier;
        bool exact_rate = false;

        Aggregator(IntervalType window_, TimestampType timestamp_scale_multiplier_, bool exact_rate_ = false)
            : window(window_), timestamp_scale_multiplier(timestamp_scale_multiplier_), exact_rate(exact_rate_)
        {
        }

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdouble-promotion"
        std::optional<ValueType> getResult(TimestampType grid_timestamp) const
        {
            const Summary combined = sliding_sum.getCurrentSum();

            if (exact_rate)
            {
                if (combined.count == 0)
                    return std::nullopt;

                const auto & last_removed = sliding_sum.getLastRemoved();
                const TimestampType last_timestamp = combined.last_timestamp;
                const ValueType last_value = combined.last_value;
                const Float64 total_resets = combined.resets;

                // Check if last_removed sample is valid and within staleness window.
                bool has_prev = false;
                ValueType prev_value = 0;
                TimestampType prev_timestamp = 0;

                if (last_removed.has_value())
                {
                    const auto & prev_summary = last_removed->second;
                    Int64 sample_ts = static_cast<Int64>(prev_summary.last_timestamp);
                    Int64 cut_off_ts = static_cast<Int64>(grid_timestamp) - static_cast<Int64>(window);
                    if (sample_ts <= cut_off_ts && (cut_off_ts - sample_ts) <= static_cast<Int64>(window))
                    {
                        has_prev = true;
                        prev_timestamp = prev_summary.last_timestamp;
                        prev_value = prev_summary.last_value;
                    }
                }

                Float64 value_difference = 0.0;
                if (has_prev)
                {
                    const TimestampType time_difference = last_timestamp - prev_timestamp;
                    if (time_difference == 0)
                        return std::nullopt;

                    Float64 resets_from_prev = (check_resets && prev_value > combined.first_value)
                        ? static_cast<Float64>(prev_value) : 0.0;
                    value_difference = last_value - prev_value + total_resets + resets_from_prev;
                }
                else
                {
                    if (combined.count < 2)
                        return std::nullopt;

                    const TimestampType time_difference = last_timestamp - combined.first_timestamp;
                    if (time_difference == 0)
                        return std::nullopt;

                    value_difference = last_value - combined.first_value + total_resets;
                }

                Float64 factor = 1.0;
                if constexpr (is_rate)
                    factor = static_cast<Float64>(timestamp_scale_multiplier) / static_cast<Float64>(window);

                value_difference *= factor;
                return static_cast<ValueType>(value_difference);
            }

            /// Need at least two samples to calculate the rate or delta.
            if (combined.count < 2)
                return std::nullopt;

            const TimestampType first_timestamp = combined.first_timestamp;
            const ValueType first_value = combined.first_value;
            const TimestampType last_timestamp = combined.last_timestamp;
            const ValueType last_value = combined.last_value;
            const UInt64 total_count = combined.count;
            const Float64 total_resets = combined.resets;

            /// Extrapolation logic follows Prometheus' rate calculation (Apache 2.0).
            const TimestampType time_difference = last_timestamp - first_timestamp;
            if (time_difference == 0)
                return std::nullopt;

            Float64 value_difference = last_value - first_value + total_resets;

            // Duration between first/last samples and boundary of range. Subtract in Int128
            // to avoid signed overflow and Float64 precision loss with large timestamps.
            Float64 duration_to_start = static_cast<Float64>(
                static_cast<Int128>(static_cast<Int64>(first_timestamp))
                - static_cast<Int128>(static_cast<Int64>(grid_timestamp))
                + static_cast<Int128>(static_cast<Int64>(window)));
            Float64 duration_to_end = static_cast<Float64>(
                static_cast<Int128>(static_cast<Int64>(grid_timestamp))
                - static_cast<Int128>(static_cast<Int64>(last_timestamp)));

            const auto sampled_interval = time_difference;
            const Float64 average_duration_between_samples = static_cast<Float64>(sampled_interval) / static_cast<Float64>(total_count - 1);

            // Extrapolate to boundary if samples are close enough (within 10% of avg duration);
            // otherwise extrapolate by half of the average duration between samples.
            const auto extrapolation_threshold = average_duration_between_samples * 1.1;
            Float64 extrapolate_to_interval = static_cast<Float64>(sampled_interval);

            if (duration_to_start >= extrapolation_threshold)
                duration_to_start = average_duration_between_samples / 2;

            if (check_resets && value_difference > 0 && first_value >= 0)
            {
                // Counters cannot be negative; extrapolate zero point if closer than
                // duration_to_start, avoiding extrapolation to negative counter values.
                Float64 duration_to_zero = static_cast<Float64>(sampled_interval) * (first_value / value_difference);
                duration_to_start = std::min(duration_to_zero, duration_to_start);
            }

            extrapolate_to_interval += duration_to_start;

            if (duration_to_end >= extrapolation_threshold)
                duration_to_end = average_duration_between_samples / 2;
            extrapolate_to_interval += duration_to_end;

            Float64 factor = extrapolate_to_interval / static_cast<Float64>(sampled_interval);

            if constexpr (is_rate)
                factor = factor * static_cast<Float64>(timestamp_scale_multiplier) / static_cast<Float64>(window);

            value_difference *= factor;

            return static_cast<ValueType>(value_difference);
        }
#pragma clang diagnostic pop
    };

    /// The bucket stores raw samples; the aggregator's `add(const Samples &)` preaggregates them into a `Summary`.
    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 4;
};


/// Aggregate function to calculate extrapolated values (rate, increase and delta) of timeseries on the specified grid
template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_rate_, bool check_resets_>
class AggregateFunctionTimeseriesExtrapolatedValue final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesExtrapolatedValue<TimestampType_, IntervalType_, ValueType_, is_rate_, check_resets_>,
        AggregateFunctionTimeseriesExtrapolatedValueTraits<TimestampType_, IntervalType_, ValueType_, is_rate_, check_resets_>>
{
public:
    using Traits = AggregateFunctionTimeseriesExtrapolatedValueTraits<TimestampType_, IntervalType_, ValueType_, is_rate_, check_resets_>;

    static constexpr bool is_rate = Traits::is_rate;
    static constexpr bool check_resets = Traits::check_resets;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesExtrapolatedValue, Traits>;
    bool exact_rate = false;

    AggregateFunctionTimeseriesExtrapolatedValue(
        const DataTypes & argument_types_,
        const Array & parameters_,
        TimestampType start_,
        TimestampType end_,
        IntervalType step_,
        IntervalType window_,
        UInt32 scale_,
        bool exact_rate_ = false)
        : Base(argument_types_, parameters_, start_, end_, step_, window_, scale_)
        , exact_rate(exact_rate_)
    {
    }

    Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return Aggregator{Base::window, Base::timestamp_scale_multiplier, exact_rate};
    }
};

/// Each SQL function as a 3-argument template with its `is_rate` / `check_resets` variant baked in, so
/// registration names the function directly.
template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesRateToGrid = AggregateFunctionTimeseriesExtrapolatedValue<TimestampType, IntervalType, ValueType, /* is_rate = */ true, /* check_resets = */ true>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesIncreaseToGrid = AggregateFunctionTimeseriesExtrapolatedValue<TimestampType, IntervalType, ValueType, /* is_rate = */ false, /* check_resets = */ true>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesDeltaToGrid = AggregateFunctionTimeseriesExtrapolatedValue<TimestampType, IntervalType, ValueType, /* is_rate = */ false, /* check_resets = */ false>;

}
