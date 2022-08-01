#pragma once

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <Columns/IColumn.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
struct Settings;


/** Aggregate function that takes arbitrary number of arbitrary arguments and does nothing.
  */
class AggregateFunctionNothing final : public IAggregateFunctionHelper<AggregateFunctionNothing>
{
public:
    AggregateFunctionNothing(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionNothing>(arguments, params) {}

    String getName() const override
    {
        return "nothing";
    }

    DataTypePtr getReturnType() const override
    {
        return argument_types.empty() ? std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>()) : argument_types.front();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict) const override
    {
    }

    void destroy(AggregateDataPtr __restrict) const noexcept override
    {
    }

    bool hasTrivialDestructor() const override
    {
        return true;
    }

    size_t sizeOfData() const override
    {
        return 0;
    }

    size_t alignOfData() const override
    {
        return 1;
    }

    void add(AggregateDataPtr __restrict, const IColumn **, size_t, Arena *) const override
    {
    }

    void merge(AggregateDataPtr __restrict, ConstAggregateDataPtr, Arena *) const override
    {
    }

    void serialize(ConstAggregateDataPtr __restrict, WriteBuffer & buf, std::optional<size_t>) const override
    {
        writeChar('\0', buf);
    }

    void deserialize(AggregateDataPtr __restrict, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        [[maybe_unused]] char symbol;
        readChar(symbol, buf);
        assert(symbol == '\0');
    }

    void insertResultInto(AggregateDataPtr __restrict, IColumn & to, Arena *) const override
    {
        to.insertDefault();
    }
};

}
