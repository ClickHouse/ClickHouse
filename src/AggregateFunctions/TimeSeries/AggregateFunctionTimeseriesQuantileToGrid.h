#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <optional>
#include <utility>

#include <Common/Exception.h>
#include <Common/NaNUtils.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/sort.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// The order of the values inside a window: NaN samples are kept and ordered before every real value, like Prometheus'
/// `vectorByValueHeap.Less`. It is a strict weak ordering (all NaNs are equivalent), which plain `<` on floats is not.
template <typename ValueType>
bool timeseriesQuantileLess(ValueType lhs, ValueType rhs)
{
    if (isNaN(lhs))
        return !isNaN(rhs);
    return !isNaN(rhs) && lhs < rhs;
}

/// R-7 (quantileExactInclusive) quantile of `sorted_values`, which must be ordered by `timeseriesQuantileLess`, with the
/// Prometheus edge cases of the level: NaN gives NaN, a level below 0 gives -Inf and a level above 1 gives +Inf.
template <typename ValueType>
std::optional<Float64> computeTimeseriesQuantile(const VectorWithMemoryTracking<ValueType> & sorted_values, Float64 phi)
{
    if (sorted_values.empty())
        return std::nullopt;

    if (std::isnan(phi))
        return std::numeric_limits<Float64>::quiet_NaN();
    if (phi < 0.0)
        return -std::numeric_limits<Float64>::infinity();
    if (phi > 1.0)
        return std::numeric_limits<Float64>::infinity();

    const size_t n = sorted_values.size();
    if (n == 1)
        return static_cast<Float64>(sorted_values[0]);

    /// rank = phi * (n - 1), interpolated between the neighbouring values.
    const Float64 rank = phi * static_cast<Float64>(n - 1);
    const size_t lower = static_cast<size_t>(std::floor(rank));
    const size_t upper = static_cast<size_t>(std::ceil(rank));

    /// An exact rank gives the sample itself. Prometheus has no such shortcut and computes `lower * 1 + upper * 0`,
    /// which is NaN when either sample is infinite.
    if (lower == upper)
        return static_cast<Float64>(sorted_values[lower]);

    /// The weighted form of Prometheus (`lower * (1 - weight) + upper * weight`) rather than `lower + weight * (upper - lower)`,
    /// so that infinite samples give the infinity instead of `Inf - Inf = NaN`.
    const Float64 weight = rank - static_cast<Float64>(lower);
    const Float64 result = static_cast<Float64>(sorted_values[lower]) * (1.0 - weight) + static_cast<Float64>(sorted_values[upper]) * weight;
    return result;
}

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

    /// The values of one bucket, or of the whole window, sorted by `timeseriesQuantileLess`. Merging two summaries is
    /// a merge of two sorted runs, and a merged summary can be taken out again by a pass over both sorted runs, so the
    /// `SlidingSum` keeps one running summary of the window and every quantile is read from it without sorting.
    struct Summary
    {
        VectorWithMemoryTracking<ValueType> values;

        /// Adds values in any order.
        void add(VectorWithMemoryTracking<ValueType> && new_values)
        {
            ::sort(new_values.begin(), new_values.end(), timeseriesQuantileLess<ValueType>);
            if (values.empty())
            {
                values = std::move(new_values);
                return;
            }

            const size_t old_size = values.size();
            values.insert(values.end(), new_values.begin(), new_values.end());
            std::inplace_merge(values.begin(), values.begin() + old_size, values.end(), timeseriesQuantileLess<ValueType>);
        }

        void merge(const Summary & other)
        {
            if (other.values.empty())
                return;

            const size_t old_size = values.size();
            values.insert(values.end(), other.values.begin(), other.values.end());
            std::inplace_merge(values.begin(), values.begin() + old_size, values.end(), timeseriesQuantileLess<ValueType>);
        }

        /// Removes the values of `leaving`, which was merged before: every value of `leaving` drops one equivalent value.
        void unmerge(const Summary & leaving, const Summary * /*new_first*/)
        {
            size_t kept = 0;
            size_t leaving_index = 0;
            for (size_t i = 0; i < values.size(); ++i)
            {
                if (leaving_index < leaving.values.size() && !timeseriesQuantileLess(values[i], leaving.values[leaving_index]))
                {
                    ++leaving_index;
                    continue;
                }
                values[kept++] = values[i];
            }

            if (leaving_index != leaving.values.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot remove values from the window of timeSeriesQuantileToGrid: they were not added");

            values.resize(kept);
        }
    };

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
            return computeTimeseriesQuantile(sliding_sum.getCurrentSum().values, phi);
        }
    };

    static constexpr UInt16 FORMAT_VERSION = 2;
};

/// The quantile level `phi` of `timeSeriesQuantileToGrid`: a number for the whole grid or an array with one number per
/// grid point. It is captured from the first added row and must be the same in every other row.
class AggregateFunctionTimeseriesQuantileToGridPhi
{
public:
    /// Captures the argument from the first included row if nothing has been captured yet, then checks that the other
    /// included rows of `[row_begin, row_end)` carry the captured value. `column` holds numbers or arrays of numbers. A row
    /// is included if its flag is non-zero and `flag_value_to_include` is true, or its flag is zero and
    /// `flag_value_to_include` is false (`flags` is nullptr when every row is included).
    void captureOrCheck(size_t grid_size, size_t row_begin, size_t row_end, const IColumn & column, const UInt8 * flags, bool flag_value_to_include);

    void merge(const AggregateFunctionTimeseriesQuantileToGridPhi & other);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf, size_t grid_size);

    /// The level at grid point `grid_index`. It is 0 if no row has been added, then every window is empty anyway.
    Float64 at(size_t grid_index) const;

private:
    /// One value if the level is the same at every grid point, `grid_size` values otherwise. Empty until a row is added.
    VectorWithMemoryTracking<Float64> values;
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
        AggregateFunctionTimeseriesQuantileToGridPhi phi;
    };

    Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return {};
    }

    void addExtraArguments(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** extra_columns,
        const UInt8 * flags, bool flag_value_to_include) const
    {
        data(place)->phi.captureOrCheck(Base::grid_size, row_begin, row_end, *extra_columns[0], flags, flag_value_to_include);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        Base::mergeImpl(place, rhs, arena);
        data(place)->phi.merge(data(rhs)->phi);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        Base::serialize(place, buf, version);
        data(place)->phi.serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        Base::deserialize(place, buf, version, arena);
        data(place)->phi.deserialize(buf, Base::grid_size);
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
