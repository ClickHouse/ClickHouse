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

template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_rate_>
struct AggregateFunctionTimeseriesExtrapolatedValueTraits
{
    static constexpr bool is_rate = is_rate_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return is_rate ? "timeSeriesRateToGrid" : "timeSeriesDeltaToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Per-bucket summary for rate/delta and, once combined over a window, the window summary too: the first and
    /// last sample, the sample count, and the reset adjustment (the sum of pre-decrease values, `rate` only - a
    /// decrease between consecutive samples is a reset; `delta`, a gauge, does not count them). `merge` adds a
    /// summary at the back of the window and `unmerge` drops one from the front, each accounting for the reset
    /// across the boundary between adjacent buckets, so a window's summary is maintained incrementally in O(1).
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
            if constexpr (is_rate)
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
                if constexpr (is_rate)
                {
                    if (leaving.last_value > new_first->first_value)
                        resets -= static_cast<Float64>(leaving.last_value);     /// drop the cross-boundary reset
                }
                first_timestamp = new_first->first_timestamp;
                first_value = new_first->first_value;
            }
        }
    };

    /// Sliding aggregator for rate/delta: preaggregates each bucket into a `Summary` and keeps the window's
    /// summary in a `SlidingSum`. `Summary` is invertible, so the window is maintained with a single running
    /// sum in O(1) per bucket; `getResult` reads its first/last sample, count and resets.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;
        VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> temp_buffer;  /// reused sort buffer

        /// `Summary::merge` is order-dependent (not commutative), so it must take the invertible running-sum path,
        /// not the two-stacks path which combines values out of time order.
        static_assert(decltype(sliding_sum)::is_invertible);

        IntervalType window;
        TimestampType timestamp_scale_multiplier;

        Aggregator(IntervalType window_, TimestampType timestamp_scale_multiplier_)
            : window(window_), timestamp_scale_multiplier(timestamp_scale_multiplier_)
        {
        }

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            /// Preaggregate the bucket's samples (visited in ascending order) into a per-bucket summary; the bucket's
            /// latest timestamp is the summary's `last_timestamp`.
            Summary summary;
            samples.forEachSampleSorted([&summary](TimestampType timestamp, ValueType value)
            {
                if (summary.count == 0)
                {
                    summary.first_timestamp = timestamp;
                    summary.first_value = value;
                }
                else if constexpr (is_rate)
                {
                    if (summary.last_value > value)
                        summary.resets += static_cast<Float64>(summary.last_value);
                }
                summary.last_timestamp = timestamp;
                summary.last_value = value;
                ++summary.count;
            }, temp_buffer);
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

            /// Need at least two samples to calculate the rate or delta.
            if (combined.count < 2)
                return std::nullopt;

            const TimestampType first_timestamp = combined.first_timestamp;
            const ValueType first_value = combined.first_value;
            const TimestampType last_timestamp = combined.last_timestamp;
            const ValueType last_value = combined.last_value;
            const UInt64 total_count = combined.count;
            const Float64 total_resets = combined.resets;

            /// The extrapolation logic is copied from Prometheus' rate calculation
            /// (https://github.com/prometheus/prometheus/blob/5e124cf4f2b9467e4ae1c679840005e727efd599/promql/functions.go#L127),
            /// licensed under the Apache License 2.0.
            const TimestampType time_difference = last_timestamp - first_timestamp;
            if (time_difference == 0)
                return std::nullopt;

            Float64 value_difference = last_value - first_value + total_resets;

            // Duration between first/last samples and boundary of range. Subtract in `Int128` first to avoid
            // both signed overflow on `grid_timestamp - window` and `Float64` precision loss when timestamps
            // are large (e.g. `DateTime64(9)` near present-day epoch ~1.7e18).
            Float64 duration_to_start = static_cast<Float64>(
                static_cast<Int128>(static_cast<Int64>(first_timestamp))
                - static_cast<Int128>(static_cast<Int64>(grid_timestamp))
                + static_cast<Int128>(static_cast<Int64>(window)));
            Float64 duration_to_end = static_cast<Float64>(
                static_cast<Int128>(static_cast<Int64>(grid_timestamp))
                - static_cast<Int128>(static_cast<Int64>(last_timestamp)));

            const auto sampled_interval = time_difference;
            const Float64 average_duration_between_samples = static_cast<Float64>(sampled_interval) / static_cast<Float64>(total_count - 1);

            // If samples are close enough to the (lower or upper) boundary of the range, we extrapolate the
            // rate all the way to the boundary in question. "Close enough" is up to 10% more than the average
            // duration between samples within the range; otherwise we extrapolate by only half of the average
            // duration between samples (our guess for where the series actually starts or ends).
            const auto extrapolation_threshold = average_duration_between_samples * 1.1;
            Float64 extrapolate_to_interval = static_cast<Float64>(sampled_interval);

            if (duration_to_start >= extrapolation_threshold)
                duration_to_start = average_duration_between_samples / 2;

            if (is_rate && value_difference > 0 && first_value >= 0)
            {
                // Counters cannot be negative. If we have any slope at all we can extrapolate the zero point
                // of the counter; if that is closer than duration_to_start, take it as the start, avoiding
                // extrapolation to negative counter values.
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
};


/// Aggregate function to calculate extrapolated values (rate and delta) of timeseries on the specified grid
template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_rate_>
class AggregateFunctionTimeseriesExtrapolatedValue final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesExtrapolatedValue<TimestampType_, IntervalType_, ValueType_, is_rate_>,
        AggregateFunctionTimeseriesExtrapolatedValueTraits<TimestampType_, IntervalType_, ValueType_, is_rate_>>
{
public:
    using Traits = AggregateFunctionTimeseriesExtrapolatedValueTraits<TimestampType_, IntervalType_, ValueType_, is_rate_>;

    static constexpr bool is_rate = Traits::is_rate;

    using TimestampType = typename Traits::TimestampType;
    using ValueType = typename Traits::ValueType;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesExtrapolatedValue, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t /* num_populated_buckets */) const
    {
        return Aggregator{Base::window, Base::timestamp_scale_multiplier};
    }

    static constexpr UInt16 FORMAT_VERSION = 3;
    static constexpr bool DateTime64Supported = true;
};

/// Each SQL function as a 3-argument template with its is_rate variant baked in, so registration names the
/// function directly.
template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesRateToGrid = AggregateFunctionTimeseriesExtrapolatedValue<TimestampType, IntervalType, ValueType, true>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesDeltaToGrid = AggregateFunctionTimeseriesExtrapolatedValue<TimestampType, IntervalType, ValueType, false>;

}
