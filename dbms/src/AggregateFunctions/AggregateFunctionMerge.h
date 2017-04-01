#pragma once

#include <DataTypes/DataTypeAggregateFunction.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>


namespace DB
{


/** Not an aggregate function, but an adapter of aggregate functions,
  * Aggregate functions with the `Merge` suffix accept `DataTypeAggregateFunction` as an argument
  * (state of the aggregate function obtained earlier using the aggregate function with the `State` suffix)
  * and combine them with aggregation.
  */

class AggregateFunctionMerge final : public IAggregateFunction
{
private:
    AggregateFunctionPtr nested_func_owner;
    IAggregateFunction * nested_func;

public:
    AggregateFunctionMerge(AggregateFunctionPtr nested_) : nested_func_owner(nested_), nested_func(nested_func_owner.get()) {}

    String getName() const override
    {
        return nested_func->getName() + "Merge";
    }

    DataTypePtr getReturnType() const override
    {
        return nested_func->getReturnType();
    }

    void setArguments(const DataTypes & arguments) override
    {
        if (arguments.size() != 1)
            throw Exception("Passed " + toString(arguments.size()) + " arguments to unary aggregate function " + this->getName(),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypeAggregateFunction * data_type = typeid_cast<const DataTypeAggregateFunction *>(&*arguments[0]);

        if (!data_type || data_type->getFunctionName() != nested_func->getName())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument for aggregate function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        nested_func->setArguments(data_type->getArgumentsDataTypes());
    }

    void setParameters(const Array & params) override
    {
        nested_func->setParameters(params);
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
        nested_func->merge(place, static_cast<const ColumnAggregateFunction &>(*columns[0]).getData()[row_num], arena);
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

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        nested_func->insertResultInto(place, to);
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }

    static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena)
    {
        static_cast<const AggregateFunctionMerge &>(*that).add(place, columns, row_num, arena);
    }

    IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }
};

}
