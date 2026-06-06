#pragma once

#include <cstddef>

#include <base/types.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/DequeWithMemoryTracking.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesOverTimeBuckets.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesOverTimeOperations.h>


namespace DB
{

/// Baseline `_over_time` driver and the `Traits` aliases used by the factory.
///
/// The `_stats` family (running-stats / mono-deque / two-pointer drivers) lives in
/// `AggregateFunctionTimeseriesOverTimeStatsAligned.h`. Bucket types and Operations
/// live in `AggregateFunctionTimeseriesOverTimeBuckets.h` and
/// `AggregateFunctionTimeseriesOverTimeOperations.h` respectively, and shared
/// numerical helpers (`quantile`) in `AggregateFunctionTimeseriesOverTimeHelpers.h`.


template <
    bool has_array_arguments,
    typename TimestampTypeT,
    typename IntervalTypeT,
    typename ValueTypeT,
    typename OperationT,
    template <typename, typename> class BucketT>
struct AggregateFunctionTimeseriesOverTimeTraits
{
    static constexpr bool array_arguments = has_array_arguments;

    using TimestampType = TimestampTypeT;
    using IntervalType = IntervalTypeT;
    using ValueType = ValueTypeT;
    using Operation = OperationT;
    using Bucket = BucketT<TimestampType, ValueType>;

    static String getName() { return Operation::getName(); }
};


/// Template aliases matching the 5-parameter template template signature required by
/// `createAggregateFunctionTimeseries`. The 5th bool parameter is unused for _over_time functions.
template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesAvgOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesAvgOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesMinOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesMinOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesMaxOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesMaxOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesSumOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesSumOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesCountOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesCountOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesStddevPopOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesStddevPopOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesStdvarPopOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesStdvarPopOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesPresentOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesPresentOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSingleSampleBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesAbsentOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesAbsentOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSingleSampleBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesFirstOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesFirstOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfLastOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesTsOfLastOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSingleSampleBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfFirstOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesTsOfFirstOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfMinOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesTsOfMinOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfMaxOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesTsOfMaxOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesQuantileOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesQuantileOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesMadOverTimeTraits = AggregateFunctionTimeseriesOverTimeTraits<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesMadOverTimeOperation<TimestampType, ValueType>,
    TimeseriesOverTimeVectorBucket>;


template <typename Traits>
class AggregateFunctionTimeseriesOverTime final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTime<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTime<Traits>, Traits>;
    using Bucket = typename Base::Bucket;

    explicit AggregateFunctionTimeseriesOverTime(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_,
        UInt32 timestamp_scale_, Float64 extra_param_ = 0)
        : Base(argument_types_, parameters_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , extra_param(extra_param_)
    {
    }

    static void serializeBucket(Bucket & bucket, WriteBuffer & buf)
    {
        bucket.serialize(buf);
    }

    void deserializeBucket(Bucket & bucket, ReadBuffer & buf, const size_t bucket_index) const
    {
        bucket.deserialize(buf, [&](TimestampType timestamp)
        {
            Base::checkTimestampInRange(timestamp, bucket_index);
        });
    }

    void doInsertResultInto(AggregateDataPtr __restrict place, IColumn & to) const
    {
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + Base::bucket_count);

        if (!Base::bucket_count)
            return;

        ColumnNullable & result_to = typeid_cast<ColumnNullable &>(arr_to.getData());
        auto & data_to = typeid_cast<typename Base::ColVecResultType &>(result_to.getNestedColumn()).getData();
        auto & nulls_to = result_to.getNullMapData();

        const size_t old_size = data_to.size();
        chassert(old_size == nulls_to.size(), "Sizes of nested column and null map of Nullable column are not equal");

        data_to.resize(old_size + Base::bucket_count);
        nulls_to.resize(old_size + Base::bucket_count);

        ValueType * values = data_to.data() + old_size;
        UInt8 * nulls = nulls_to.data() + old_size;

        auto & buckets = Base::data(place)->buckets;

        DequeWithMemoryTracking<std::pair<TimestampType, ValueType>> samples_in_window;

        for (UInt32 i = 0; i < Base::bucket_count; ++i)
        {
            const TimestampType current_timestamp = Base::start_timestamp + i * Base::step;

            /// Pop before ingesting the current bucket so nothing outside the window is ever observed by `fillResultValue`.
            while (!samples_in_window.empty() && samples_in_window.front().first + Base::window <= current_timestamp)
                samples_in_window.pop_front();

            auto bucket_it = buckets.find(i);
            if (bucket_it != buckets.end())
                bucket_it->second.appendActiveSamplesToWindow(samples_in_window, current_timestamp, Base::window);

            Traits::Operation::fillResultValue(samples_in_window, values[i], nulls[i], extra_param, Base::timestamp_scale_multiplier);
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 1;

private:
    Float64 extra_param;
};

}
