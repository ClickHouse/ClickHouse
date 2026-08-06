#pragma once

#include <cmath>
#include <cstddef>
#include <limits>
#include <optional>

#include <base/sort.h>

#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

/// Implements PromQL `quantile_over_time(phi, v[w])` on a grid (timeSeriesQuantileToGrid).
///
/// A quantile is not decomposable, so unlike the `*_over_time` family in
/// AggregateFunctionTimeseriesOverTime.h the bucket keeps the raw samples
/// (`AggregateFunctionTimeseriesSamples`, like rate/increase do) and each grid point recomputes the quantile
/// over the samples of the buckets in its window - the same O(window) work per point Prometheus itself does.
///
/// The quantile definition matches Prometheus's `quantile()`: linear interpolation between the two nearest
/// ranks of the ascending-sorted window values; `phi` is NOT clamped - NaN yields NaN, `phi < 0` yields -Inf,
/// `phi > 1` yields +Inf (still only for non-empty windows; an empty window yields NULL like everywhere else).
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
struct AggregateFunctionTimeseriesQuantileTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName() { return "timeSeriesQuantileToGrid"; }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Sliding aggregator: keeps pointers to the in-window buckets. The bucket map is not mutated while
    /// `doInsertResultInto` runs, so the pointers stay valid for the whole finalization; keeping pointers
    /// avoids copying every bucket's sample set into the window.
    struct Aggregator
    {
        Float64 quantile_level;
        DequeWithMemoryTracking<std::pair<TimestampType, const Samples *>> window;
        mutable VectorWithMemoryTracking<Float64> values_buffer;    /// reused scratch for sorting the window's values

        explicit Aggregator(Float64 quantile_level_) : quantile_level(quantile_level_) {}

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            window.emplace_back(bucket_end_timestamp, &samples);
        }

        void removeBefore(TimestampType cut_off)
        {
            while (!window.empty() && window.front().first <= cut_off)
                window.pop_front();
        }

        std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/) const
        {
            values_buffer.clear();
            for (const auto & [_, samples] : window)
                samples->forEachSample([this](TimestampType, ValueType value)
                {
                    values_buffer.push_back(static_cast<Float64>(value));
                });

            if (values_buffer.empty())
                return std::nullopt;

            /// Prometheus's quantile(): unclamped phi, linear interpolation between the closest ranks.
            if (std::isnan(quantile_level))
                return static_cast<ValueType>(std::numeric_limits<Float64>::quiet_NaN());
            if (quantile_level < 0)
                return static_cast<ValueType>(-std::numeric_limits<Float64>::infinity());
            if (quantile_level > 1)
                return static_cast<ValueType>(std::numeric_limits<Float64>::infinity());

            /// NaN samples sort first, like Prometheus's `vectorByValueHeap.Less` (NaN counts as smaller than
            /// any number). The default `<` would not be a strict weak ordering in the presence of NaN.
            ::sort(values_buffer.begin(), values_buffer.end(), [](Float64 lhs, Float64 rhs)
            {
                if (std::isnan(lhs))
                    return !std::isnan(rhs);
                return lhs < rhs;
            });

            const Float64 rank = quantile_level * static_cast<Float64>(values_buffer.size() - 1);
            const size_t lower_index = static_cast<size_t>(std::floor(rank));
            const size_t upper_index = std::min(values_buffer.size() - 1, lower_index + 1);
            const Float64 weight = rank - std::floor(rank);
            return static_cast<ValueType>(values_buffer[lower_index] * (1 - weight) + values_buffer[upper_index] * weight);
        }
    };

    /// The bucket stores raw samples: the quantile needs every value in the window.
    using Bucket = Samples;
};


template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesQuantile final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesQuantile<TimestampType_, IntervalType_, ValueType_>,
        AggregateFunctionTimeseriesQuantileTraits<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesQuantileTraits<TimestampType_, IntervalType_, ValueType_>;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesQuantile, Traits>;

    explicit AggregateFunctionTimeseriesQuantile(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_, UInt32 timestamp_scale_,
        Float64 quantile_level_)
        : Base(argument_types_, parameters_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , quantile_level(quantile_level_)
    {
    }

    Aggregator createAggregator(size_t /*num_populated_buckets*/) const
    {
        return Aggregator{quantile_level};
    }

    static constexpr UInt16 FORMAT_VERSION = 1;
    static constexpr bool DateTime64Supported = true;

private:
    const Float64 quantile_level{};
};

}
