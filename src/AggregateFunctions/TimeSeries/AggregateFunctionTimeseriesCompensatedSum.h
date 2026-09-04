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
    ///
    /// Once the sum of finite samples would overflow, the summary switches to a running mean, which cannot overflow.
    /// `timeSeriesAvgToGrid` then matches Prometheus, whose `avg_over_time` of two 1e308 samples is 1e308.
    /// `timeSeriesSumToGrid` returns `mean * count`, which recovers a transient overflow
    /// (1e308 + 1e308 - 1e308 is 1e308).
    struct Summary
    {
        /// The compensated sum of the samples, or their compensated mean once `is_mean` is set.
        Float64 sum = 0;
        Float64 compensation = 0;
        UInt64 count = 0;
        bool is_mean = false;

        void add(TimestampType /*timestamp*/, ValueType sample)
        {
            const Float64 x = static_cast<Float64>(sample);
            if (is_mean)
                addToMean(x, /* other_compensation = */ 0, /* other_count = */ 1, /* other_is_mean = */ true);
            else
                addCompensated(x, /* other_compensation = */ 0, /* other_count = */ 1);
        }

        void merge(const Summary & added)
        {
            if (added.count == 0)
                return;
            if (is_mean || added.is_mean)
                addToMean(added.sum, added.compensation, added.count, added.is_mean);
            else
                addCompensated(added.sum, added.compensation, added.count);
        }

        Float64 getSum() const
        {
            const Float64 value = sum + compensation;
            return is_mean ? value * static_cast<Float64>(count) : value;
        }

        Float64 getMean() const
        {
            const Float64 value = sum + compensation;
            return is_mean ? value : value / static_cast<Float64>(count);
        }

    private:
        /// Kahan-Babuska-Neumaier step: adds `other_sum`, which stands for `other_count` samples and carries its own
        /// compensation.
        void addCompensated(Float64 other_sum, Float64 other_compensation, UInt64 other_count)
        {
            const Float64 t = sum + other_sum;
            if (!std::isfinite(t))
            {
                /// In sum mode, when the sum of finite values would overflow, it continues as a mean instead.
                if (!is_mean && std::isfinite(sum) && std::isfinite(other_sum))
                {
                    addToMean(other_sum, other_compensation, other_count, /* other_is_mean = */ false);
                    return;
                }
                /// An infinite operand is not an overflow and propagates as it is. A non-finite sum keeps no
                /// compensation: `Inf - Inf` is NaN, and Prometheus returns such a sum as is.
                compensation = 0;
            }
            else if (std::abs(sum) >= std::abs(other_sum))
                compensation += (sum - t) + other_sum + other_compensation;
            else
                compensation += (other_sum - t) + sum + other_compensation;
            sum = t;
            count += other_count;
        }

        /// Adds `other_sum`, the mean (or the sum, when `other_is_mean` is false) of `other_count` samples, as a
        /// weighted mean: mean' = mean * n / (n + other_count) + other_mean * other_count / (n + other_count).
        /// Each term is at most the magnitude of its side's mean, so this cannot overflow.
        void addToMean(Float64 other_sum, Float64 other_compensation, UInt64 other_count, bool other_is_mean)
        {
            const Float64 n = static_cast<Float64>(count);
            const Float64 total = n + static_cast<Float64>(other_count);
            /// A sum of `count` samples divided by `total` is its mean times `n / total`.
            if (is_mean)
                scale(n / total);
            else
                scale(1 / total);
            is_mean = true;
            const Float64 weight = other_is_mean ? static_cast<Float64>(other_count) / total : 1 / total;
            addCompensated(other_sum * weight, other_compensation * weight, other_count);
        }

        /// Multiplies the compensated sum by `factor`.
        void scale(Float64 factor)
        {
            sum *= factor;
            compensation *= factor;
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
                return static_cast<ResultType>(combined.getMean());
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
