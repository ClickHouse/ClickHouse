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
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

struct Settings;

/// Aggregate function to store last (latest by timestamp) 2 samples with distinct timestamps
template <typename TimestampType, typename ValueType>
class AggregateFunctionLast2Samples final :
    public IAggregateFunctionHelper<AggregateFunctionLast2Samples<TimestampType, ValueType>>
{
public:
    static constexpr bool DateTime64Supported = true;

    using Base = IAggregateFunctionHelper<AggregateFunctionLast2Samples<TimestampType, ValueType>>;

    using ColVecType = ColumnVectorOrDecimal<TimestampType>;
    using ColVecResultType = ColumnVectorOrDecimal<ValueType>;

    String getName() const override
    {
        return "timeSeriesLastTwoSamples";
    }

    /// Stores two samples with most recent distinct timestamps
    /// If there are two samples with the same timestamp, the one with bigger value is stored
    struct Data
    {
        ValueType values[2];            /// In common scenario values are Float64, so put them first as they need 8-byte alignment
        TimestampType timestamps[2];    /// Timestamps might be stored as DateTime64, DateTime32 or even as 16-bit delta from the base timestamp of the grid
        UInt16 filled = 0;              /// Number of samples stored: 0, 1 or 2

        void add(TimestampType timestamp, ValueType value)
        {
            if (filled == 0)
            {
                timestamps[0] = timestamp;
                values[0] = value;
                filled = 1;
            }
            else if (filled == 1)
            {
                if (timestamp > timestamps[0])
                {
                    /// Move the first sample to the second one
                    timestamps[1] = timestamps[0];
                    values[1] = values[0];
                    /// Set the first sample to the new one
                    timestamps[0] = timestamp;
                    values[0] = value;

                    filled = 2;
                }
                else if (timestamp == timestamps[0])
                {
                    /// Replace the value with bigger one
                    values[0] = std::max(value, values[0]);
                }
                else
                {
                    /// Add new sample as the second one
                    timestamps[1] = timestamp;
                    values[1] = value;

                    filled = 2;
                }
            }
            else
            {
                chassert(filled == 2);
                if (timestamp < timestamps[1])
                {
                    /// Nothing to do if the timestamp is less than the smallest one
                }
                else if (timestamp > timestamps[0])
                {
                    /// Move the first sample to the second one
                    timestamps[1] = timestamps[0];
                    values[1] = values[0];
                    /// Set the first sample to the new one
                    timestamps[0] = timestamp;
                    values[0] = value;
                }
                else if (timestamp == timestamps[0])
                {
                    /// Replace first sample value with bigger one
                    values[0] = std::max(value, values[0]);
                }
                else if (timestamp > timestamps[1])
                {
                    /// Replace the second sample
                    timestamps[1] = timestamp;
                    values[1] = value;
                }
                else if (timestamp == timestamps[1])
                {
                    /// Replace second sample value with bigger one
                    values[1] = std::max(value, values[1]);
                }
            }
        }

        void merge(const Data & rhs)
        {
            for (size_t i = 0; i < rhs.filled; ++i)
                add(rhs.timestamps[i], rhs.values[i]);
        }
    };

    explicit AggregateFunctionLast2Samples(const DataTypes & argument_types_)
        : Base(argument_types_, {}, createResultType(argument_types_))
    {
    }

    static DataTypePtr createResultType(const DataTypes & argument_types_)
    {
        const auto & timestamp_type = argument_types_[0];
        const auto & value_type = argument_types_[1];

        return std::make_shared<DataTypeArray>(
                make_shared<DataTypeTuple>(DataTypes{timestamp_type, value_type}));
    }

    bool allocatesMemoryInArena() const override { return false; }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<Data>;
    }

    size_t alignOfData() const override
    {
        return alignof(Data);
    }

    size_t sizeOfData() const override
    {
        return sizeof(Data);
    }

    void create(AggregateDataPtr __restrict place) const override   /// NOLINT(readability-non-const-parameter)
    {
        new (place) Data{};
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        data(place).~Data();
    }

    static Data & data(AggregateDataPtr __restrict place)   /// NOLINT(readability-non-const-parameter)
    {
        return *reinterpret_cast<Data *>(place);
    }

    static const Data & data(ConstAggregateDataPtr __restrict place)
    {
        return *reinterpret_cast<const Data *>(place);
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr __restrict place, TimestampType timestamp, ValueType value) const
    {
        data(place).add(timestamp, value);
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
        data(place).merge(data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const Data & data = this->data(place);

        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        writeBinaryLittleEndian(data.filled, buf);

        for (size_t i = 0; i < data.filled; ++i)
        {
            writeBinaryLittleEndian(data.timestamps[i], buf);
        }

        for (size_t i = 0; i < data.filled; ++i)
        {
            writeBinaryLittleEndian(data.values[i], buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        UInt16 format_version;
        readBinaryLittleEndian(format_version, buf);

        if (format_version != FORMAT_VERSION)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot deserialize data with different format version, expected {}, got {}",
                FORMAT_VERSION, format_version);

        Data & data = this->data(place);
        readBinaryLittleEndian(data.filled, buf);

        if (data.filled > 2)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot deserialize data with different bucket count, expected 2, got {}",
                data.filled);

        for (size_t i = 0; i < data.filled; ++i)
        {
            readBinaryLittleEndian(data.timestamps[i], buf);
            if (i > 0 && data.timestamps[i] > data.timestamps[i - 1])
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Timestamps must be in descending order, but got {} after {}",
                    static_cast<Int64>(data.timestamps[i]), static_cast<Int64>(data.timestamps[i - 1]));
        }

        for (size_t i = 0; i < data.filled; ++i)
        {
            readBinaryLittleEndian(data.values[i], buf);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & array_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = array_to.getOffsets();

        ColumnTuple & tuple = typeid_cast<ColumnTuple &>(array_to.getData());

        if (tuple.tupleSize() != 2)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected tuple size 2, got {}",
                tuple.tupleSize());

        ColVecType & timestamps_to = typeid_cast<ColVecType &>(tuple.getColumn(0));
        ColVecResultType & values_to = typeid_cast<ColVecResultType &>(tuple.getColumn(1));

        const Data & data = this->data(place);

        offsets_to.push_back(offsets_to.back() + data.filled);

        if (!data.filled)
            return;

        for (size_t i = 0; i < data.filled; ++i)
        {
            timestamps_to.insert(data.timestamps[i]);
            values_to.insert(data.values[i]);
        }
    }

private:
    static constexpr UInt16 FORMAT_VERSION = 1;
};

}
