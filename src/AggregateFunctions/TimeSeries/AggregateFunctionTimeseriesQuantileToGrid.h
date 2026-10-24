#pragma once

#include <cstddef>
#include <optional>
#include <utility>

#include <Common/VectorWithMemoryTracking.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesGridArgument.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSortedValues.h>


namespace DB
{

template <typename TimestampType_, typename ValueType_>
struct AggregateFunctionTimeseriesQuantileToGridTraits
{
    using GridScaleTimestampType = DateTime64;
    using TimestampType = TimestampType_;
    using ValueType = ValueType_;

    /// The quantile is interpolated between the samples, so it's calculated with double precision.
    using ResultType = Float64;

    static String getName()
    {
        return "timeSeriesQuantileToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// The bucket stores raw samples: the timestamps are needed to collapse duplicate timestamps into one sample.
    using Bucket = Samples;

    /// The sorted values of the window, so every quantile is read from them without sorting.
    using Summary = AggregateFunctionTimeseriesSortedValues<ValueType>;

    /// Sliding aggregator: keeps the sorted values of the window and reads the phi-quantile (R-7, inclusive) from them.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<Summary> sliding_sum;

        static_assert(decltype(sliding_sum)::is_invertible);

        void add(const Samples & samples, GridScaleTimestampType bucket_end_timestamp)
        {
            VectorWithMemoryTracking<ValueType> values;
            samples.forEachSample([&values](TimestampType /*timestamp*/, ValueType value)
            {
                values.push_back(value);
            });

            if (values.empty())
                return;

            Summary summary;
            summary.add(std::move(values));
            sliding_sum.add(std::move(summary), bucket_end_timestamp);
        }

        void removeBefore(GridScaleTimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<ResultType> getResult(GridScaleTimestampType /*grid_timestamp*/, Float64 phi) const
        {
            return sliding_sum.getCurrentSum().quantile(phi);
        }
    };

    static constexpr UInt16 FORMAT_VERSION = 2;
};

/// Aggregate function that computes the phi-quantile of time series values on a regular time grid.
/// Returns the R-7 (inclusive) quantile of all sample values within each grid point's window. The quantile level is the
/// argument after the samples: a number, or an array with one level per grid point.
template <typename TimestampType_, typename ValueType_>
class AggregateFunctionTimeseriesQuantileToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesQuantileToGrid<TimestampType_, ValueType_>,
        AggregateFunctionTimeseriesQuantileToGridTraits<TimestampType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesQuantileToGridTraits<TimestampType_, ValueType_>;

    using TimestampType = typename Traits::TimestampType;
    using ValueType = typename Traits::ValueType;
    using ResultType = typename Traits::ResultType;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesQuantileToGrid, Traits>;
    using Base::Base;

    /// The quantile level `phi` is one more argument after the samples, kept in the state.
    static constexpr size_t num_extra_arguments = 1;

    struct State : Base::State
    {
        AggregateFunctionTimeseriesGridArgument phi;
    };

    Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return {};
    }

    void addExtraArguments(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** extra_columns,
        const UInt8 * flags, bool flag_value_to_include) const
    {
        data(place)->phi.captureOrCheck(Traits::getName(), "phi", Base::grid_size, row_begin, row_end, *extra_columns[0], flags, flag_value_to_include);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        Base::mergeImpl(place, rhs, arena);
        data(place)->phi.merge(Traits::getName(), "phi", data(rhs)->phi);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        Base::serialize(place, buf, version);
        data(place)->phi.serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        Base::deserialize(place, buf, version, arena);
        data(place)->phi.deserialize("phi", buf, Base::grid_size);
    }

    std::optional<ResultType> getGridPointResult(const Aggregator & aggregator, ConstAggregateDataPtr place, size_t grid_index) const
    {
        return aggregator.getResult(Base::getGridPoint(grid_index), data(place)->phi.at(grid_index));
    }

private:
    static const State * data(ConstAggregateDataPtr __restrict place)
    {
        return reinterpret_cast<const State *>(place);
    }

    static State * data(AggregateDataPtr __restrict place)
    {
        return reinterpret_cast<State *>(place);
    }
};

}
