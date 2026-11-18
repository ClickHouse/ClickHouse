#pragma once

#include <cstddef>
#include <cstring>
#include <optional>
#include <type_traits>


#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionLast2Samples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>


namespace DB
{

template <typename TimestampType_, typename ValueType_, bool is_rate_>
struct AggregateFunctionTimeseriesInstantValueTraits
{
    static constexpr bool is_rate = is_rate_;
    using GridScaleTimestampType = DateTime64;
    using GridScaleIntervalType = Decimal64;
    using ValueType = ValueType_;
    using TimestampType = TimestampType_;
    using ResultType = Float64;

    static String getName()
    {
        return is_rate_ ? "timeSeriesInstantRateToGrid" : "timeSeriesInstantDeltaToGrid";
    }

    using Summary = typename AggregateFunctionLast2Samples<TimestampType, ValueType>::Data;

    /// Sliding aggregator: `irate`/`idelta` use only the two most recent samples in the window, which is exactly
    /// what `Last2Samples::Data` keeps. Buckets are added in time order, so merging each new bucket keeps the
    /// window's newest two samples; a single `Data` is enough. `removeBefore` drops samples that fell out of the
    /// window (once the newest is out, the whole window is empty).
    struct Aggregator
    {
        Summary latest;
        Int64 column_to_grid_multiplier;
        Int64 column_ticks_per_second;

        Aggregator(Int64 column_to_grid_multiplier_, Int64 column_ticks_per_second_)
            : column_to_grid_multiplier(column_to_grid_multiplier_), column_ticks_per_second(column_ticks_per_second_)
        {
        }

        void add(const Summary & summary, GridScaleTimestampType /*bucket_end_timestamp*/)
        {
            latest.merge(summary);
        }

        void removeBefore(GridScaleTimestampType cut_off)
        {
            /// `timestamps[0]` is the newest sample, `timestamps[1]` the previous one.
            if (latest.filled >= 1 && static_cast<Int64>(latest.timestamps[0]) * column_to_grid_multiplier <= cut_off)
                latest.filled = 0;
            else if (latest.filled == 2 && static_cast<Int64>(latest.timestamps[1]) * column_to_grid_multiplier <= cut_off)
                latest.filled = 1;
        }

        std::optional<ResultType> getResult(GridScaleTimestampType /*grid_timestamp*/) const
        {
            if (latest.filled < 2)
                return std::nullopt;

            const Float64 value = static_cast<Float64>(latest.values[0]);
            const Float64 previous_value = static_cast<Float64>(latest.values[1]);

            /// The timestamps of the samples have the scale of the input columns.
            const Int64 time_difference = static_cast<Int64>(latest.timestamps[0]) - static_cast<Int64>(latest.timestamps[1]);
            if (time_difference == 0)
                return std::nullopt;

            /// Resets are taken into account for `irate` (counter) but not for `idelta` (gauge).
            const Float64 value_difference = (is_rate && value < previous_value) ? value : (value - previous_value);

            if constexpr (is_rate)
                return value_difference * static_cast<Float64>(column_ticks_per_second) / static_cast<Float64>(time_difference);
            else
                return value_difference;
        }
    };

    /// InstantValue keeps no preaggregated summary - the bucket (its two newest samples) is fed to the aggregator as-is.
    using Bucket = Summary;

    static constexpr UInt16 FORMAT_VERSION = 5;
};


/// Aggregate function to calculate instant values (irate and idelta) of timeseries on the specified grid
template <typename TimestampType_, typename ValueType_, bool is_rate_>
class AggregateFunctionTimeseriesInstantValue final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesInstantValue<TimestampType_, ValueType_, is_rate_>,
        AggregateFunctionTimeseriesInstantValueTraits<TimestampType_, ValueType_, is_rate_>>
{
public:
    using Traits = AggregateFunctionTimeseriesInstantValueTraits<TimestampType_, ValueType_, is_rate_>;

    static constexpr bool is_rate = Traits::is_rate;
    using GridScaleTimestampType = typename Traits::GridScaleTimestampType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesInstantValue, Traits>;
    using Base::Base;

    typename Traits::Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return typename Traits::Aggregator{Base::column_to_grid_multiplier, Base::column_ticks_per_second};
    }
};

/// Each SQL function as a template with its is_rate variant baked in, so registration names the
/// function directly.
template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesInstantRateToGrid = AggregateFunctionTimeseriesInstantValue<TimestampType, ValueType, true>;

template <typename TimestampType, typename ValueType>
using AggregateFunctionTimeseriesInstantDeltaToGrid = AggregateFunctionTimeseriesInstantValue<TimestampType, ValueType, false>;

}
