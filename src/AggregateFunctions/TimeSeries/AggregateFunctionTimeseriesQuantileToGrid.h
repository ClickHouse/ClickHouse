#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <optional>

#include <Common/VectorWithMemoryTracking.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

template <typename TimestampType_, typename IntervalType_, typename ValueType_>
struct AggregateFunctionTimeseriesQuantileToGridTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return "timeSeriesQuantileToGrid";
    }

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Per-bucket summary: collects all values in the bucket. Non-invertible (no `unmerge`),
    /// so the SlidingSum uses the recompute or two-stack path.
    struct Summary
    {
        VectorWithMemoryTracking<ValueType> values;

        void merge(const Summary & other)
        {
            values.insert(values.end(), other.values.begin(), other.values.end());
        }

        void serialize(WriteBuffer & buf) const
        {
            writeBinaryLittleEndian(static_cast<UInt64>(values.size()), buf);
            for (const auto & v : values)
                writeBinaryLittleEndian(v, buf);
        }

        void deserialize(ReadBuffer & buf)
        {
            UInt64 size = 0;
            readBinaryLittleEndian(size, buf);
            values.resize(size);
            for (auto & v : values)
                readBinaryLittleEndian(v, buf);
        }

        template <typename RangeType>
        void checkTimestampsInRange(const RangeType & /*range*/) const
        {
        }
    };

    /// Sliding aggregator: collects all values in the window and computes the phi-quantile (R-7, inclusive).
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;
        Float64 phi;

        Aggregator(size_t stack_size, Float64 phi_)
            : sliding_sum(stack_size), phi(phi_)
        {
        }

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            Summary summary;
            samples.forEachSample([&summary](TimestampType /*timestamp*/, ValueType value)
            {
                summary.values.push_back(value);
            });
            if (summary.values.empty())
                return;
            add(std::move(summary), bucket_end_timestamp);
        }

        void add(Summary summary, TimestampType bucket_end_timestamp)
        {
            if (summary.values.empty())
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
            if (combined.values.empty())
                return std::nullopt;

            const size_t n = combined.values.size();
            if (n == 1)
                return combined.values[0];

            /// Copy and sort for quantile computation.
            std::vector<ValueType> sorted_values(combined.values.begin(), combined.values.end());
            std::sort(sorted_values.begin(), sorted_values.end());

            /// R-7 quantile (quantileExactInclusive): rank = phi * (n - 1), linear interpolation.
            /// Clamp rank to [0, n-1] for out-of-range phi; the translator wraps the output to handle
            /// phi < 0 (-Inf), phi > 1 (+Inf), and NaN phi.
            Float64 rank = phi * static_cast<Float64>(n - 1);
            if (std::isnan(rank))
                return static_cast<ValueType>(std::numeric_limits<Float64>::quiet_NaN());
            if (rank < 0.0)
                rank = 0.0;
            else if (rank > static_cast<Float64>(n - 1))
                rank = static_cast<Float64>(n - 1);

            const size_t lower = static_cast<size_t>(std::floor(rank));
            const size_t upper = static_cast<size_t>(std::ceil(rank));

            if (lower == upper)
                return sorted_values[lower];

            const Float64 fraction = rank - static_cast<Float64>(lower);
            const Float64 result = static_cast<Float64>(sorted_values[lower])
                + fraction * (static_cast<Float64>(sorted_values[upper]) - static_cast<Float64>(sorted_values[lower]));
            return static_cast<ValueType>(result);
        }
    };

    using Bucket = Samples;
};


/// Aggregate function that computes the phi-quantile of time series values on a regular time grid.
/// Returns the R-7 (inclusive) quantile of all sample values within each grid point's window.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesQuantileToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesQuantileToGrid<TimestampType_, IntervalType_, ValueType_>,
        AggregateFunctionTimeseriesQuantileToGridTraits<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesQuantileToGridTraits<TimestampType_, IntervalType_, ValueType_>;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesQuantileToGrid, Traits>;

    explicit AggregateFunctionTimeseriesQuantileToGrid(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_, UInt32 timestamp_scale_, Float64 phi_)
        : Base(argument_types_, parameters_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , phi(phi_)
    {
    }

    static constexpr size_t AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS = 10;
    static constexpr size_t BPW_TO_FORCE_TWO_STACKS = 20;

    Aggregator createAggregator(size_t num_populated_buckets) const
    {
        const size_t avg_buckets_in_window = Base::bucket_count
            ? static_cast<size_t>(static_cast<double>(Base::buckets_per_window) * static_cast<double>(num_populated_buckets)
                / static_cast<double>(Base::bucket_count))
            : 0;
        const bool use_two_stacks = avg_buckets_in_window >= AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS
            || Base::buckets_per_window >= BPW_TO_FORCE_TWO_STACKS;
        const size_t stack_size = use_two_stacks ? std::min(Base::buckets_per_window, num_populated_buckets) : 0;
        return Aggregator{stack_size, phi};
    }

    static constexpr UInt16 FORMAT_VERSION = 1;
    static constexpr bool DateTime64Supported = true;

protected:
    const Float64 phi{};
};

}
