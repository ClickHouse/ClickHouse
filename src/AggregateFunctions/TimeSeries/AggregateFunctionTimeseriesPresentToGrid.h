#pragma once

#include <cstddef>
#include <optional>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSlidingSum.h>


namespace DB
{

template <typename TimestampType_, typename IntervalType_, typename ValueType_>
struct AggregateFunctionTimeseriesPresentToGridTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return "timeSeriesPresentToGrid";
    }

    /// Per-bucket summary: just the number of samples in the bucket. Invertible, so the window summary
    /// can be maintained incrementally with a single running count.
    struct Summary
    {
        UInt64 count = 0;

        void add(TimestampType /*timestamp*/, ValueType /*value*/)
        {
            ++count;
        }

        void addMany(const TimestampType * /*timestamp_ptr*/, const ValueType * /*value_ptr*/, size_t batch_size)
        {
            count += batch_size;
        }

        void merge(const Summary & other)
        {
            count += other.count;
        }

        void unmerge(const Summary & leaving, const Summary * /*new_first*/)
        {
            count -= leaving.count;
        }

        void serialize(WriteBuffer & buf) const
        {
            writeBinaryLittleEndian(count, buf);
        }

        void deserialize(ReadBuffer & buf)
        {
            readBinaryLittleEndian(count, buf);
        }

        template <typename RangeType>
        void checkTimestampsInRange(const RangeType & /*range*/) const
        {
        }
    };

    /// Sliding aggregator: the result at a grid point is 1 if the window contains at least one sample.
    struct Aggregator
    {
        AggregateFunctionTimeseriesSlidingSum<TimestampType, Summary> sliding_sum;

        void add(const Summary & summary, TimestampType bucket_end_timestamp)
        {
            if (summary.count == 0)
                return;
            sliding_sum.add(Summary{summary.count}, bucket_end_timestamp);
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
            return ValueType{1};
        }
    };

    using Bucket = Summary;

    static constexpr UInt16 FORMAT_VERSION = 1;
};


/// Aggregate function to check for presence of time series samples on the specified grid.
/// Returns 1 for each grid point whose window contains at least one sample, NULL otherwise.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesPresentToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesPresentToGrid<TimestampType_, IntervalType_, ValueType_>,
        AggregateFunctionTimeseriesPresentToGridTraits<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesPresentToGridTraits<TimestampType_, IntervalType_, ValueType_>;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesPresentToGrid, Traits>;
    using Base::Base;

    static constexpr UInt16 FORMAT_VERSION = Traits::FORMAT_VERSION;

    typename Traits::Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return {};
    }

    static constexpr bool DateTime64Supported = true;
};

}
