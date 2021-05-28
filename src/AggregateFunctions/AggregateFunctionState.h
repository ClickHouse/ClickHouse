
#pragma once

#include <DataTypes/DataTypeAggregateFunction.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/assert_cast.h>


namespace DB
{


/** Not an aggregate function, but an adapter of aggregate functions,
  * Aggregate functions with the `State` suffix differ from the corresponding ones in that their states are not finalized.
  * Return type - DataTypeAggregateFunction.
  */

class AggregateFunctionState final : public IAggregateFunctionHelper<AggregateFunctionState>
{
private:
    AggregateFunctionPtr nested_func;
    DataTypes arguments;
    Array params;

public:
    AggregateFunctionState(AggregateFunctionPtr nested_, const DataTypes & arguments_, const Array & params_)
        : IAggregateFunctionHelper<AggregateFunctionState>(arguments_, params_)
        , nested_func(nested_), arguments(arguments_), params(params_) {}

    String getName() const override
    {
        return nested_func->getName() + "State";
    }

    DataTypePtr getReturnType() const override;

    void create(AggregateDataPtr place) const override
    {
        nested_func->create(place);
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        nested_func->destroy(place);
    }

    bool hasTrivialDestructor() const override
    {
        return nested_func->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return nested_func->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_func->alignOfData();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        nested_func->add(place, columns, row_num, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_func->merge(place, rhs, arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        nested_func->serialize(place, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        nested_func->deserialize(place, buf, arena);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnAggregateFunction &>(to).getData().push_back(place);
    }

    /// Aggregate function or aggregate function state.
    bool isState() const override { return true; }

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }

    AggregateFunctionPtr getNestedFunction() const { return nested_func; }
};

}
