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


namespace DB
{

template <typename TimestampType_, typename IntervalType_>
struct AggregateFunctionTimeseriesHistogramLastToGridTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;

    static String getName()
    {
        return "timeSeriesHistogramLastToGrid";
    }

    using Bucket = TimeSeriesHistogramBucket<TimestampType>;

    /// Sliding aggregator: the result is the most recent in-window sample. Buckets are added in time order, so keeping a pointer
    /// to the newest populated bucket suffices (stable: the map is not modified while the aggregator runs).
    struct Aggregator
    {
        const Bucket * latest = nullptr;

        void add(const Bucket & bucket, TimestampType /*bucket_end_timestamp*/)
        {
            /// Buckets arrive in ascending time order, so a populated bucket's sample is newer than the kept one;
            /// `>=` makes the last write win on duplicate timestamps, matching `addSample`.
            if (bucket.has_value && (!latest || bucket.newest_timestamp >= latest->newest_timestamp))
                latest = &bucket;
        }

        void removeBefore(TimestampType cut_off)
        {
            if (latest && latest->newest_timestamp <= cut_off)
                latest = nullptr;
        }

        const Bucket * getResult(TimestampType /*grid_timestamp*/) const
        {
            return latest;
        }
    };
};


/// Aggregate function resampling native-histogram samples to the grid with staleness: the most recent
/// in-window sample per grid point, NULL where missing.
template <typename TimestampType_, typename IntervalType_>
class AggregateFunctionTimeseriesHistogramLastToGrid final :
    public AggregateFunctionTimeseriesHistogramBase<
        AggregateFunctionTimeseriesHistogramLastToGrid<TimestampType_, IntervalType_>,
        AggregateFunctionTimeseriesHistogramLastToGridTraits<TimestampType_, IntervalType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesHistogramLastToGridTraits<TimestampType_, IntervalType_>;

    using Base = AggregateFunctionTimeseriesHistogramBase<AggregateFunctionTimeseriesHistogramLastToGrid, Traits>;
    using Base::Base;

    using typename Base::TimestampType;
    using typename Base::Bucket;

    typename Traits::Aggregator createAggregator(size_t /* num_populated_buckets */) const
    {
        return {};
    }

    static constexpr UInt16 FORMAT_VERSION = 1;
    static constexpr bool DateTime64Supported = true;

    /// Constructs the result array for one grid: the sliding `Aggregator` reads off each grid point's newest in-window sample;
    /// keep in sync with `AggregateFunctionTimeseriesBase::doInsertResultInto` (NULL slot = null bit plus a default payload).
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
        auto aggregator = createAggregator(buckets.size());

        auto store_grid_result = [&](size_t grid_index)
        {
            const Bucket * result = aggregator.getResult(Base::timestampAtIndex(grid_index));
            if (result)
            {
                Base::appendBucketToResultColumns(state, *result, tuple_to);
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

}
