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

/// `is_rate` mirrors upstream's isRate: `irate` is reset-aware (on a counter reset the result is the
/// newest sample) and divides by the interval in seconds; `idelta` always subtracts.
template <typename TimestampType_, typename IntervalType_, bool is_rate_>
struct AggregateFunctionTimeseriesHistogramInstantValueTraits
{
    static constexpr bool is_rate = is_rate_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;

    static String getName()
    {
        return is_rate_ ? "timeSeriesHistogramInstantRateToGrid" : "timeSeriesHistogramInstantDeltaToGrid";
    }

    using Bucket = TimeSeriesHistogramSamplesBucket;
};


/// Aggregate functions computing instant values (`irate` and `idelta`) of native-histogram series on a grid, mirroring
/// `instantValue` (see TimeSeriesHistogramRate.h): the two most recent samples' difference, WITHOUT extrapolation; NULL below two samples.
template <typename TimestampType_, typename IntervalType_, bool is_rate_>
class AggregateFunctionTimeseriesHistogramInstantValue final :
    public AggregateFunctionTimeseriesHistogramBase<
        AggregateFunctionTimeseriesHistogramInstantValue<TimestampType_, IntervalType_, is_rate_>,
        AggregateFunctionTimeseriesHistogramInstantValueTraits<TimestampType_, IntervalType_, is_rate_>>
{
public:
    using Traits = AggregateFunctionTimeseriesHistogramInstantValueTraits<TimestampType_, IntervalType_, is_rate_>;

    static constexpr bool is_rate = Traits::is_rate;

    using Base = AggregateFunctionTimeseriesHistogramBase<AggregateFunctionTimeseriesHistogramInstantValue, Traits>;
    using Base::Base;

    using typename Base::TimestampType;
    using typename Base::Bucket;
    using typename Base::State;

    /// Sliding aggregator: every sample of the window in ascending timestamp order; `getResult`
    /// decodes the two most recent ones and runs the upstream algorithm.
    struct Aggregator
    {
        const State * state;
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

        std::optional<TimeSeriesFloatHistogram> getResult(TimestampType /*grid_timestamp*/) const
        {
            if (window_samples.samples.size() < 2)
                return std::nullopt;

            auto newest = window_samples.samples.rbegin();
            auto previous = std::next(newest);
            return timeSeriesHistogramInstantValue(
                Base::histogramFromRecord(*state, previous->second), previous->first,
                Base::histogramFromRecord(*state, newest->second), newest->first,
                timestamp_scale, is_rate);
        }
    };

    static constexpr UInt16 FORMAT_VERSION = 1;
    static constexpr bool DateTime64Supported = true;

    /// Constructs the result array for one grid; keep in sync with `AggregateFunctionTimeseriesHistogramLastToGrid::doInsertResultInto`
    /// (see the extrapolated sibling `AggregateFunctionTimeseriesHistogramExtrapolatedValue` for details).
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
        Aggregator aggregator{&state, static_cast<Float64>(Base::timestamp_scale_multiplier), {}};

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

/// Each SQL function as a 2-argument template with its is_rate variant baked in, so registration
/// names the function directly.
template <typename TimestampType, typename IntervalType>
using AggregateFunctionTimeseriesHistogramInstantRateToGrid = AggregateFunctionTimeseriesHistogramInstantValue<TimestampType, IntervalType, true>;

template <typename TimestampType, typename IntervalType>
using AggregateFunctionTimeseriesHistogramInstantDeltaToGrid = AggregateFunctionTimeseriesHistogramInstantValue<TimestampType, IntervalType, false>;

}
