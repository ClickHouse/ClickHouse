#pragma once

#include <cstddef>
#include <optional>
#include <utility>

#include <base/sort.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHistogramBase.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Functions/TimeSeries/TimeSeriesHistogramRate.h>


namespace DB
{

/// `is_rate` divides the extrapolated value by the window's seconds (`rate` vs `increase`); `check_resets` mirrors upstream's
/// isCounter: resets are re-added and extrapolation clamps at the counter's zero point (`rate`/`increase` vs `delta` over gauges).
template <typename TimestampType_, typename IntervalType_, bool is_rate_, bool check_resets_>
struct AggregateFunctionTimeseriesHistogramExtrapolatedValueTraits
{
    static constexpr bool is_rate = is_rate_;
    static constexpr bool check_resets = check_resets_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;

    static String getName()
    {
        if constexpr (is_rate)
            return "timeSeriesHistogramRateToGrid";
        else if constexpr (check_resets)
            return "timeSeriesHistogramIncreaseToGrid";
        else
            return "timeSeriesHistogramDeltaToGrid";
    }

    using Bucket = TimeSeriesHistogramSamplesBucket;
};


/// Aggregate functions computing extrapolated values (`rate`, `increase`, `delta`) of native-histogram series on a grid, mirroring
/// PromQL's `histogramRate` + `extrapolatedRate` (see TimeSeriesHistogramRate.h); the result is a synthetic gauge histogram or NULL.
template <typename TimestampType_, typename IntervalType_, bool is_rate_, bool check_resets_>
class AggregateFunctionTimeseriesHistogramExtrapolatedValue final :
    public AggregateFunctionTimeseriesHistogramBase<
        AggregateFunctionTimeseriesHistogramExtrapolatedValue<TimestampType_, IntervalType_, is_rate_, check_resets_>,
        AggregateFunctionTimeseriesHistogramExtrapolatedValueTraits<TimestampType_, IntervalType_, is_rate_, check_resets_>>
{
public:
    using Traits = AggregateFunctionTimeseriesHistogramExtrapolatedValueTraits<TimestampType_, IntervalType_, is_rate_, check_resets_>;

    static constexpr bool is_rate = Traits::is_rate;
    static constexpr bool check_resets = Traits::check_resets;

    using Base = AggregateFunctionTimeseriesHistogramBase<AggregateFunctionTimeseriesHistogramExtrapolatedValue, Traits>;
    using Base::Base;

    using typename Base::TimestampType;
    using typename Base::IntervalType;
    using typename Base::Bucket;
    using typename Base::State;

    /// Sliding aggregator: every sample of the window in ascending timestamp order; `getResult`
    /// decodes them into the kernel's histograms and runs the upstream algorithm.
    struct Aggregator
    {
        const State * state;
        IntervalType window;
        Float64 timestamp_scale;
        TimeSeriesHistogramWindowSamples<TimestampType> window_samples;

        void add(const Bucket & bucket, TimestampType /*bucket_end_timestamp*/)
        {
            window_samples.add(*state, bucket);
        }

        void removeBefore(TimestampType cut_off)
        {
            window_samples.removeBefore(cut_off);
        }

        std::optional<TimeSeriesFloatHistogram> getResult(TimestampType grid_timestamp) const
        {
            if (window_samples.samples.size() < 2)
                return std::nullopt;

            std::vector<std::pair<TimestampType, TimeSeriesFloatHistogram>> points; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
            points.reserve(window_samples.samples.size());
            for (const auto & [timestamp, record] : window_samples.samples)
                points.emplace_back(timestamp, Base::histogramFromRecord(*state, record));
            return timeSeriesHistogramExtrapolatedRate(points, grid_timestamp, window, timestamp_scale, is_rate, check_resets);
        }
    };

    static constexpr UInt16 FORMAT_VERSION = 1;
    static constexpr bool DateTime64Supported = true;

    /// Constructs the result array for one grid: the sliding `Aggregator` computes each grid point's extrapolated
    /// value; keep in sync with `AggregateFunctionTimeseriesHistogramLastToGrid::doInsertResultInto`.
    void doInsertResultInto(AggregateDataPtr __restrict place, IColumn & to) const
    {
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.empty() ? Base::grid_size : offsets_to.back() + Base::grid_size);

        if (!Base::grid_size)
            return;

        ColumnNullable & result_to = typeid_cast<ColumnNullable &>(arr_to.getData());
        ColumnTuple & tuple_to = typeid_cast<ColumnTuple &>(result_to.getNestedColumn());
        auto & nulls_to = result_to.getNullMapData();

        const auto & state = *Base::data(place);
        const auto & buckets = state.buckets;
        Aggregator aggregator{&state, Base::window, static_cast<Float64>(Base::timestamp_scale_multiplier), {}};

        auto store_grid_result = [&](size_t grid_index)
        {
            auto result = aggregator.getResult(Base::timestampAtIndex(grid_index));
            if (result)
            {
                Base::appendHistogramToResultColumns(*result, tuple_to);
                nulls_to.push_back(UInt8{0});
            }
            else
            {
                /// Null bit plus a default payload (zeros and empty arrays).
                result_to.insertDefault();
            }
        };

        /// Visit the populated buckets in ascending index order, feeding each into the sliding window; when most bucket slots
        /// are populated (`use_range_scan`) a hash-map lookup per index beats collecting and sorting them once.
        const bool use_range_scan = (buckets.size() != 0)
            && (static_cast<double>(buckets.size()) >= static_cast<double>(Base::bucket_count) * Base::BUCKET_DENSITY_TO_ENABLE_RANGE_SCAN);
        if (use_range_scan)
        {
            size_t next_bucket = 0;
            for (size_t grid_index = 0; grid_index < Base::grid_size; ++grid_index)
            {
                const size_t window_end = Base::bucketRangeInWindow(grid_index).second;
                for (; next_bucket < window_end; ++next_bucket)
                {
                    const auto * it = buckets.find(next_bucket);
                    if (it)
                        aggregator.add(it->getMapped(), Base::bucketEndTimestamp(next_bucket));
                }
                Base::removeOutOfWindow(aggregator, grid_index);
                store_grid_result(grid_index);
            }
        }
        else
        {
            VectorWithMemoryTracking<std::pair<size_t, const Bucket *>> ordered_buckets;
            ordered_buckets.reserve(buckets.size());
            for (const auto & entry : buckets)
                ordered_buckets.emplace_back(entry.getKey(), &entry.getMapped());
            ::sort(ordered_buckets.begin(), ordered_buckets.end(),
                [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

            size_t pos = 0;
            for (size_t grid_index = 0; grid_index < Base::grid_size; ++grid_index)
            {
                const size_t window_end = Base::bucketRangeInWindow(grid_index).second;
                for (; pos < ordered_buckets.size() && ordered_buckets[pos].first < window_end; ++pos)
                    aggregator.add(*ordered_buckets[pos].second, Base::bucketEndTimestamp(ordered_buckets[pos].first));
                Base::removeOutOfWindow(aggregator, grid_index);
                store_grid_result(grid_index);
            }
        }
    }
};

/// Each SQL function as a 2-argument template with its `is_rate` / `check_resets` variant baked in, so
/// registration names the function directly.
template <typename TimestampType, typename IntervalType>
using AggregateFunctionTimeseriesHistogramRateToGrid = AggregateFunctionTimeseriesHistogramExtrapolatedValue<TimestampType, IntervalType, /* is_rate = */ true, /* check_resets = */ true>;

template <typename TimestampType, typename IntervalType>
using AggregateFunctionTimeseriesHistogramIncreaseToGrid = AggregateFunctionTimeseriesHistogramExtrapolatedValue<TimestampType, IntervalType, /* is_rate = */ false, /* check_resets = */ true>;

template <typename TimestampType, typename IntervalType>
using AggregateFunctionTimeseriesHistogramDeltaToGrid = AggregateFunctionTimeseriesHistogramExtrapolatedValue<TimestampType, IntervalType, /* is_rate = */ false, /* check_resets = */ false>;

}
