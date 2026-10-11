#pragma once

#include <cstddef>
#include <optional>
#include <utility>

#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesGridArgument.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template <typename TimestampType_, typename ValueType_>
struct AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits
{
    using GridScaleTimestampType = DateTime64;
    using TimestampType = TimestampType_;
    using ValueType = ValueType_;
    using ResultType = Float64;

    static String getName()
    {
        return "timeSeriesDoubleExponentialSmoothingToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// The bucket's sample values, or the window's values once merged. They stay in timestamp order because
    /// each bucket yields sorted samples and the window merges buckets in time order.
    struct Summary
    {
        VectorWithMemoryTracking<ValueType> samples;

        void merge(const Summary & other)
        {
            samples.insert(samples.end(), other.samples.begin(), other.samples.end());
        }
    };

    /// Sliding aggregator: keeps the window's samples in a `SlidingSum` and computes Prometheus's
    /// `double_exponential_smoothing` (Holt-Winters double exponential smoothing) per grid point.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<Summary> sliding_sum;

        void add(const Samples & samples, GridScaleTimestampType bucket_end_timestamp)
        {
            Summary summary;
            samples.forEachSample([&summary](TimestampType, ValueType value)
            {
                summary.samples.push_back(value);
            });
            add(std::move(summary), bucket_end_timestamp);
        }

        void add(Summary summary, GridScaleTimestampType bucket_end_timestamp)
        {
            if (summary.samples.empty())
                return;
            sliding_sum.add(std::move(summary), bucket_end_timestamp);
        }

        void removeBefore(GridScaleTimestampType cut_off)
        {
            sliding_sum.removeBefore(cut_off);
        }

        std::optional<ResultType> getResult(GridScaleTimestampType /*grid_timestamp*/, Float64 sf, Float64 tf) const
        {
            const auto & values = sliding_sum.getCurrentSum().samples;
            if (values.empty())
                return std::nullopt;

            /// Like Prometheus, the factors are checked only for a window with samples, and a NaN factor passes the check.
            if (sf <= 0 || sf >= 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function {} expects smoothing factor in the open interval (0, 1), got {}", getName(), sf);
            if (tf <= 0 || tf >= 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function {} expects trend factor in the open interval (0, 1), got {}", getName(), tf);

            /// Prometheus can't smooth with fewer than two points, and returns no value in that case.
            if (values.size() < 2)
                return std::nullopt;

            const size_t l = values.size();

            /// Initial level and trend, matching Prometheus's funcDoubleExponentialSmoothing.
            Float64 s0 = 0;
            Float64 s1 = static_cast<Float64>(values[0]);
            Float64 b = static_cast<Float64>(values[1]) - static_cast<Float64>(values[0]);

            for (size_t i = 1; i < l; ++i)
            {
                const Float64 x = sf * static_cast<Float64>(values[i]);

                /// calcTrendValue(i - 1): the trend is left unchanged on the first step (i == 1), then updated as
                /// tf * (s1 - s0) + (1 - tf) * b on subsequent steps.
                if (i != 1)
                    b = tf * (s1 - s0) + (1.0 - tf) * b;

                const Float64 y = (1.0 - sf) * (s1 + b);
                s0 = s1;
                s1 = x + y;
            }

            return s1;
        }
    };

    /// The bucket stores raw samples; the aggregator's `add(const Samples &)` collects their timestamps and values.
    using Bucket = Samples;

    static constexpr UInt16 FORMAT_VERSION = 1;
};


/// Prometheus `double_exponential_smoothing` over a sliding window on a regular time grid.
/// The smoothing and trend factors follow the samples: each is a number or an array with one number per grid point.
template <typename TimestampType_, typename ValueType_>
class AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid<TimestampType_, ValueType_>,
        AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits<TimestampType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesDoubleExponentialSmoothingToGridTraits<TimestampType_, ValueType_>;

    using TimestampType = typename Traits::TimestampType;
    using ValueType = typename Traits::ValueType;
    using Aggregator = typename Traits::Aggregator;

    using ResultType = typename Traits::ResultType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid, Traits>;
    using Base::Base;

    /// The smoothing factor and the trend factor are two more arguments after the samples, kept in the state.
    static constexpr size_t num_extra_arguments = 2;

    struct State : Base::State
    {
        AggregateFunctionTimeseriesGridArgument smoothing_factor;
        AggregateFunctionTimeseriesGridArgument trend_factor;
    };

    Aggregator createAggregator(size_t /* num_populated_buckets */) const
    {
        return {};
    }

    void addExtraArguments(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** extra_columns,
        const UInt8 * flags, bool flag_value_to_include) const
    {
        data(place)->smoothing_factor.captureOrCheck(
            Traits::getName(), "smoothing_factor", Base::grid_size, row_begin, row_end, *extra_columns[0], flags, flag_value_to_include);
        data(place)->trend_factor.captureOrCheck(
            Traits::getName(), "trend_factor", Base::grid_size, row_begin, row_end, *extra_columns[1], flags, flag_value_to_include);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        Base::mergeImpl(place, rhs, arena);
        data(place)->smoothing_factor.merge(Traits::getName(), "smoothing_factor", data(rhs)->smoothing_factor);
        data(place)->trend_factor.merge(Traits::getName(), "trend_factor", data(rhs)->trend_factor);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        Base::serialize(place, buf, version);
        data(place)->smoothing_factor.serialize(buf);
        data(place)->trend_factor.serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        Base::deserialize(place, buf, version, arena);
        data(place)->smoothing_factor.deserialize("smoothing_factor", buf, Base::grid_size);
        data(place)->trend_factor.deserialize("trend_factor", buf, Base::grid_size);
    }

    std::optional<ResultType> getGridPointResult(const Aggregator & aggregator, ConstAggregateDataPtr place, size_t grid_index) const
    {
        return aggregator.getResult(
            Base::getGridPoint(grid_index), data(place)->smoothing_factor.at(grid_index), data(place)->trend_factor.at(grid_index));
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
