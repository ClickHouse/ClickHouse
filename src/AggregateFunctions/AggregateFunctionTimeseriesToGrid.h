#pragma once

#include <cstddef>
#include <cstring>
#include <memory>
#include <type_traits>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include "config.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

struct Settings;

/// Aggregate function to convert timeseries to the specified grid with staleness
template <typename TimestampType, typename IntervalType, typename ValueType>
class AggregateFunctionTimeseriesToGrid final :
    public IAggregateFunctionHelper<AggregateFunctionTimeseriesToGrid<TimestampType, IntervalType, ValueType>>
{
public:
    static constexpr bool DateTime64Supported = true;

    using Base = IAggregateFunctionHelper<AggregateFunctionTimeseriesToGrid<TimestampType, IntervalType, ValueType>>;

    using ColVecType = ColumnVectorOrDecimal<TimestampType>;
    using ColVecResultType = ColumnVectorOrDecimal<ValueType>;

    String getName() const override
    {
        return "tsToGrid";
    }

    explicit AggregateFunctionTimeseriesToGrid(const DataTypes & argument_types_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType staleness_)
        : Base(argument_types_, {}, createResultType(argument_types_))
        , start_timestamp(start_timestamp_)
        , end_timestamp(end_timestamp_)
        , step(step_)
        , staleness(staleness_)
        , bucket_count(bucketCount(start_timestamp, end_timestamp, step))
        , values_offset(valuesOffset(bucket_count))
        , size_of_data(values_offset + sizeof(ValueType) * bucket_count)
    {
        if (staleness < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Staleness should be non-negative");
    }

    static DataTypePtr createResultType(const DataTypes & argument_types_)
    {
        DataTypePtr res = argument_types_[1];

        return std::make_shared<DataTypeArray>(res);
    }

    bool allocatesMemoryInArena() const override { return false; }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<TimestampType> && std::is_trivially_destructible_v<ValueType>;
    }

    size_t alignOfData() const override
    {
        return std::max(alignof(TimestampType), alignof(ValueType));
    }

    size_t sizeOfData() const override
    {
        return size_of_data;
    }

    static size_t bucketCount(TimestampType start_timestamp, TimestampType end_timestamp, IntervalType step)
    {
        if (end_timestamp < start_timestamp)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "End timestamp is less than start timestamp");

        if (step <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");

        return (end_timestamp - start_timestamp) / step + 1;
    }

    static size_t valuesOffset(size_t bucket_count)
    {
        const size_t timestamps_size = sizeof(TimestampType) * bucket_count;
        const size_t values_alignment = alignof(ValueType);

        /// Align values to the alignment of ValueType
        return (timestamps_size + values_alignment - 1) / values_alignment * values_alignment;
    }

    TimestampType * ALWAYS_INLINE timestamps(AggregateDataPtr __restrict place) const
    {
        return reinterpret_cast<TimestampType *>(place);
    }

    const TimestampType * ALWAYS_INLINE timestamps(ConstAggregateDataPtr __restrict place) const
    {
        return reinterpret_cast<const TimestampType *>(place);
    }

    ValueType * ALWAYS_INLINE values(AggregateDataPtr __restrict place) const
    {
        return reinterpret_cast<ValueType *>(place + values_offset);
    }

    const ValueType * ALWAYS_INLINE values(ConstAggregateDataPtr __restrict place) const
    {
        return reinterpret_cast<const ValueType *>(place + values_offset);
    }

    void create(AggregateDataPtr __restrict place) const override  /// NOLINT
    {
        for (size_t i = 0; i < bucket_count; ++i)
        {
            new (place + i * sizeof(TimestampType)) TimestampType{};
        }
        for (size_t i = 0; i < bucket_count; ++i)
        {
            new (place + values_offset + i * sizeof(ValueType)) ValueType{};
        }
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < bucket_count; ++i)
        {
            timestamps(place)[i].~TimestampType();
        }
        for (size_t i = 0; i < bucket_count; ++i)
        {
            values(place)[i].~ValueType();
        }
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr __restrict place, TimestampType timestamp, ValueType value) const
    {
        if (timestamp + staleness < start_timestamp || timestamp > end_timestamp)
            return;

        const size_t index = (timestamp < start_timestamp) ? 0 : ((timestamp - start_timestamp + step - 1) / step);
        if (timestamps(place)[index] < timestamp)
        {
            timestamps(place)[index] = timestamp;
            values(place)[index] = value;
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & timestamp_column = typeid_cast<const ColVecType &>(*columns[0]);
        const auto & value_column = typeid_cast<const ColVecResultType &>(*columns[1]);
        add(place, timestamp_column.getData()[row_num], value_column.getData()[row_num]);
    }

    void addMany(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, size_t start, size_t end) const
    {
        for (size_t i = start; i < end; ++i)
            add(place, timestamp_ptr[i], value_ptr[i]);
    }

    void addManyNotNull(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, const UInt8 * __restrict null_map, size_t start, size_t end) const
    {
        for (size_t i = start; i < end; ++i)
            if (!null_map[i])
                add(place, timestamp_ptr[i], value_ptr[i]);
    }

    void addManyConditional(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, const UInt8 * __restrict condition_map, size_t start, size_t end) const
    {
        for (size_t i = start; i < end; ++i)
            if (condition_map[i])
                add(place, timestamp_ptr[i], value_ptr[i]);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const auto & timestamp_column = typeid_cast<const ColVecType &>(*columns[0]);
        const auto & value_column = typeid_cast<const ColVecResultType &>(*columns[1]);
        if (if_argument_pos >= 0)
        {
            const auto & flags = typeid_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            addManyConditional(place, timestamp_column.getData().data(), value_column.getData().data(), flags.data(), row_begin, row_end);
        }
        else
        {
            addMany(place, timestamp_column.getData().data(), value_column.getData().data(), row_begin, row_end);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos)
        const override
    {
        const auto & timestamp_column = typeid_cast<const ColVecType &>(*columns[0]);
        const auto & value_column = typeid_cast<const ColVecResultType &>(*columns[1]);
        if (if_argument_pos >= 0)
        {
            /// Merge the 2 sets of flags (null and if) into a single one. This allows us to use parallelizable sums when available
            const auto * if_flags = typeid_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            auto final_flags = std::make_unique<UInt8[]>(row_end);
            for (size_t i = row_begin; i < row_end; ++i)
                final_flags[i] = (!null_map[i]) & !!if_flags[i];

            addManyConditional(place, timestamp_column.getData().data(), value_column.getData().data(), final_flags.get(), row_begin, row_end);
        }
        else
        {
            addManyNotNull(place, timestamp_column.getData().data(), value_column.getData().data(), null_map, row_begin, row_end);
        }
    }

    void addManyDefaults(
        AggregateDataPtr __restrict /*place*/,
        const IColumn ** /*columns*/,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
    }

    void addBatchSparse(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena) const override
    {
        const auto & column_sparse = typeid_cast<const ColumnSparse &>(*columns[0]);
        const auto * values = &column_sparse.getValuesColumn();
        const auto & offsets = column_sparse.getOffsetsData();

        size_t from = std::lower_bound(offsets.begin(), offsets.end(), row_begin) - offsets.begin();
        size_t to = std::lower_bound(offsets.begin(), offsets.end(), row_end) - offsets.begin();

        for (size_t i = from; i < to; ++i)
            add(places[offsets[i]] + place_offset, &values, i + 1, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        for (size_t i = 0; i < bucket_count; ++i)
        {
            if (timestamps(place)[i] < timestamps(rhs)[i])
            {
                timestamps(place)[i] = timestamps(rhs)[i];
                values(place)[i] = values(rhs)[i];
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        writeBinaryLittleEndian(bucket_count, buf);

        for (size_t i = 0; i < bucket_count; ++i)
        {
            writeBinaryLittleEndian(timestamps(place)[i], buf);
        }

        for (size_t i = 0; i < bucket_count; ++i)
        {
            writeBinaryLittleEndian(values(place)[i], buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        UInt16 format_version;
        readBinaryLittleEndian(format_version, buf);

        if (format_version != FORMAT_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with different format version");

        size_t size;
        readBinaryLittleEndian(size, buf);

        if (size != bucket_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot deserialize data with different bucket count");

        for (size_t i = 0; i < size; ++i)
        {
            readBinaryLittleEndian(timestamps(place)[i], buf);
        }

        for (size_t i = 0; i < size; ++i)
        {
            readBinaryLittleEndian(values(place)[i], buf);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + bucket_count);

        if (!bucket_count)
            return;

        auto & data_to = typeid_cast<ColVecResultType &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + bucket_count);

        TimestampType current_timestamp = start_timestamp;
        size_t previous_timestamp_index = 0;
        for (size_t i = 0; i < bucket_count; ++i, current_timestamp += step)
        {
            ValueType value = values(place)[i];
            if (timestamps(place)[i] != 0)
                previous_timestamp_index = i;
            else
            {
                /// Use the previous value if the current timestamp is missing and the previous one is not stale
                if (timestamps(place)[previous_timestamp_index] + staleness >= current_timestamp)
                    value = values(place)[previous_timestamp_index];
            }

            data_to[old_size + i] = value;
        }
    }

    void insertResultIntoBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        IColumn & to,
        Arena * arena) const override
    {
        const size_t batch_size = row_end - row_begin;

        /// Reserve offsets and values in column to
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        auto & data_to = typeid_cast<ColVecResultType &>(arr_to.getData()).getData();
        offsets_to.reserve(offsets_to.size() + batch_size);
        data_to.reserve(data_to.size() + batch_size * bucket_count);

        Base::insertResultIntoBatch(row_begin, row_end, places, place_offset, to, arena);
    }

private:
    static constexpr UInt16 FORMAT_VERSION = 1;

    const TimestampType start_timestamp{};
    const TimestampType end_timestamp{};
    const IntervalType step{};
    const IntervalType staleness{};
    const size_t bucket_count{};
    const size_t values_offset{};
    const size_t size_of_data{};
};

}
