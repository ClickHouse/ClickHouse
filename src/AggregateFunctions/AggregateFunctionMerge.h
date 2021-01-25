#pragma once

#include <DataTypes/DataTypeAggregateFunction.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/** Not an aggregate function, but an adapter of aggregate functions,
  * Aggregate functions with the `Merge` suffix accept `DataTypeAggregateFunction` as an argument
  * (state of the aggregate function obtained earlier using the aggregate function with the `State` suffix)
  * and combine them with aggregation.
  */

class AggregateFunctionMerge final : public IAggregateFunctionHelper<AggregateFunctionMerge>
{
private:
    AggregateFunctionPtr nested_func;

public:
    AggregateFunctionMerge(const AggregateFunctionPtr & nested_, const DataTypePtr & argument)
        : IAggregateFunctionHelper<AggregateFunctionMerge>({argument}, nested_->getParameters())
        , nested_func(nested_)
    {
        const DataTypeAggregateFunction * data_type = typeid_cast<const DataTypeAggregateFunction *>(argument.get());

        if (!data_type || data_type->getFunctionName() != nested_func->getName())
            throw Exception("Illegal type " + argument->getName() + " of argument for aggregate function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override
    {
        return nested_func->getName() + "Merge";
    }

    DataTypePtr getReturnType() const override
    {
        return nested_func->getReturnType();
    }

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
        nested_func->merge(place, assert_cast<const ColumnAggregateFunction &>(*columns[0]).getData()[row_num], arena);
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

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertResultInto(place, to, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }
};

}
