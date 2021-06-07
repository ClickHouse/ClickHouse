#pragma once

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <Columns/IColumn.h>
#include <AggregateFunctions/IAggregateFunction.h>


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
        return argument_types.front();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr) const override
    {
    }

    void destroy(AggregateDataPtr) const noexcept override
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

    void add(AggregateDataPtr, const IColumn **, size_t, Arena *) const override
    {
    }

    void merge(AggregateDataPtr, ConstAggregateDataPtr, Arena *) const override
    {
    }

    void serialize(ConstAggregateDataPtr, WriteBuffer &) const override
    {
    }

    void deserialize(AggregateDataPtr, ReadBuffer &, Arena *) const override
    {
    }

    void insertResultInto(AggregateDataPtr, IColumn & to, Arena *) const override
    {
        to.insertDefault();
    }
};

}
