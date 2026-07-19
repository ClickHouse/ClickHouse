#pragma once

#include <bit>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <type_traits>


#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>

#include <optional>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_resets_>
struct AggregateFunctionTimeseriesChangesTraits
{
    static constexpr bool is_resets = is_resets_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return is_resets ? "timeSeriesResetsToGrid" : "timeSeriesChangesToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// A Prometheus stale marker (bit pattern 0x7ff0000000000002) means "the series has gone stale here",
    /// so such samples must be ignored entirely by `changes`/`resets`.
    static bool isPrometheusStaleNaN(ValueType value)
    {
        if constexpr (std::is_same_v<ValueType, Float64>)
            return std::bit_cast<UInt64>(value) == 0x7ff0000000000002ULL;
        return false;
    }

    /// Per-bucket summary for changes/resets and, once combined over a window, the window summary too: the first
    /// and last value, the sample count, and the transition count. `merge` adds a summary at the back of the window
    /// and `unmerge` drops one from the front, each accounting for the transition across the boundary between
    /// adjacent buckets, so a window's transition count is maintained incrementally in O(1).
    struct Summary
    {
        ValueType first_value = 0;
        ValueType last_value = 0;
        UInt64 count = 0;
        UInt64 changes = 0;

        /// Whether the transition prev -> curr is counted: a decrease for resets, any change otherwise.
        static bool isCounted(ValueType prev, ValueType curr)
        {
            if constexpr (is_resets)
                return curr < prev;
            else
                /// PromQL `changes` treats consecutive regular NaN samples as the same value.
                return curr != prev && !(std::isnan(curr) && std::isnan(prev));
        }

        void merge(const Summary & added)
        {
            if (added.count == 0)
                return;
            if (count == 0)
            {
                *this = added;
                return;
            }
            if (isCounted(last_value, added.first_value))
                ++changes;      /// transition across the bucket boundary
            changes += added.changes;
            last_value = added.last_value;
            count += added.count;
        }

        void unmerge(const Summary & leaving, const Summary * new_first)
        {
            changes -= leaving.changes;
            count -= leaving.count;
            if (new_first)
            {
                if (isCounted(leaving.last_value, new_first->first_value))
                    --changes;      /// drop the transition across the boundary
                first_value = new_first->first_value;   /// the window's first value is now the new front's
            }
        }
    };

    /// Sliding aggregator for changes/resets: preaggregates each bucket into a `Summary` and keeps the
    /// window's summary in a `SlidingSum`. `Summary` is invertible, so the window is maintained with a single
    /// running sum in O(1) per bucket.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;
        VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> temp_buffer;  /// reused sort buffer

        /// `Summary::merge` is order-dependent (not commutative), so it must take the invertible running-sum path,
        /// not the two-stacks path which combines values out of time order.
        static_assert(decltype(sliding_sum)::is_invertible);

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            /// Preaggregate the bucket's samples (visited in ascending order) into a per-bucket summary.
            Summary summary;
            samples.forEachSampleSorted([&summary](TimestampType, ValueType value)
            {
                if (isPrometheusStaleNaN(value))
                    return;
                if (summary.count == 0)
                    summary.first_value = value;
                else if (Summary::isCounted(summary.last_value, value))
                    ++summary.changes;
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

        std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/) const
        {
            const Summary combined = sliding_sum.getCurrentSum();
            if (combined.count == 0)
                return std::nullopt;
            return static_cast<ValueType>(combined.changes);
        }
    };

    /// The bucket stores raw samples; the aggregator's `add(const Samples &)` preaggregates them into a `Summary`.
    using Bucket = Samples;
};


template <typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_resets_>
class AggregateFunctionTimeseriesChanges final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesChanges<TimestampType_, IntervalType_, ValueType_, is_resets_>,
        AggregateFunctionTimeseriesChangesTraits<TimestampType_, IntervalType_, ValueType_, is_resets_>>
{
public:
    using Traits = AggregateFunctionTimeseriesChangesTraits<TimestampType_, IntervalType_, ValueType_, is_resets_>;

    static constexpr bool is_resets = Traits::is_resets;

    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesChanges, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t /* num_populated_buckets */) const
    {
        return {};
    }

    static constexpr UInt16 FORMAT_VERSION = 2;
    static constexpr bool DateTime64Supported = true;
};

/// Each SQL function as a 3-argument template with its is_resets variant baked in, so registration names the
/// function directly.
template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesChangesToGrid = AggregateFunctionTimeseriesChanges<TimestampType, IntervalType, ValueType, false>;

template <typename TimestampType, typename IntervalType, typename ValueType>
using AggregateFunctionTimeseriesResetsToGrid = AggregateFunctionTimeseriesChanges<TimestampType, IntervalType, ValueType, true>;

}
