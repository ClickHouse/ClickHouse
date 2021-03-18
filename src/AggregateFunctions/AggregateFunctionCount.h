#pragma once

#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

#include <array>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/assert_cast.h>


namespace DB
{

struct AggregateFunctionCountData
{
    UInt64 count = 0;
};

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/// Simply count number of calls.
class AggregateFunctionCount final : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCount>
{
public:
    AggregateFunctionCount(const DataTypes & argument_types_) : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String getName() const override { return "count"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void add(AggregateDataPtr place, const IColumn **, size_t, Arena *) const override
    {
        ++data(place).count;
    }

    void addBatchSinglePlace(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena *, ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            data(place).count += countBytesInFilter(flags);
        }
        else
        {
            data(place).count += batch_size;
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            data(place).count += countBytesInFilterWithNull(flags, null_map);
        }
        else
        {
            data(place).count += batch_size - countBytesInFilter(null_map, batch_size);
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeVarUInt(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readVarUInt(data(place).count, buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(data(place).count);
    }

    /// Reset the state to specified value. This function is not the part of common interface.
    void set(AggregateDataPtr place, UInt64 new_count)
    {
        data(place).count = new_count;
    }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr &, const DataTypes & types, const Array & params, const AggregateFunctionProperties & /*properties*/) const override;
};


/// Simply count number of not-NULL values.
class AggregateFunctionCountNotNullUnary final
    : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCountNotNullUnary>
{
public:
    AggregateFunctionCountNotNullUnary(const DataTypePtr & argument, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCountNotNullUnary>({argument}, params)
    {
        if (!argument->isNullable())
            throw Exception("Logical error: not Nullable data type passed to AggregateFunctionCountNotNullUnary", ErrorCodes::LOGICAL_ERROR);
    }

    String getName() const override { return "count"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        data(place).count += !assert_cast<const ColumnNullable &>(*columns[0]).isNullAt(row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeVarUInt(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readVarUInt(data(place).count, buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(data(place).count);
    }
};

}
