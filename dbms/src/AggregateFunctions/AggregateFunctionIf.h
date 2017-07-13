#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{


/** Not an aggregate function, but an adapter of aggregate functions,
  * which any aggregate function `agg(x)` makes an aggregate function of the form `aggIf(x, cond)`.
  * The adapted aggregate function takes two arguments - a value and a condition,
  * and calculates the nested aggregate function for the values when the condition is satisfied.
  * For example, avgIf(x, cond) calculates the average x if `cond`.
  */
class AggregateFunctionIf final : public IAggregateFunction
{
private:
    AggregateFunctionPtr nested_func_owner;
    IAggregateFunction * nested_func;
    size_t num_agruments;
public:
    AggregateFunctionIf(AggregateFunctionPtr nested_) : nested_func_owner(nested_), nested_func(nested_func_owner.get()) {}

    String getName() const override
    {
        return nested_func->getName() + "If";
    }

    DataTypePtr getReturnType() const override
    {
        return nested_func->getReturnType();
    }

    void setArguments(const DataTypes & arguments) override
    {
        num_agruments = arguments.size();

        if (!typeid_cast<const DataTypeUInt8 *>(&*arguments[num_agruments - 1]))
            throw Exception("Illegal type " + arguments[num_agruments - 1]->getName() + " of second argument for aggregate function " + getName() + ". Must be UInt8.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        DataTypes nested_arguments;
        for (size_t i = 0; i < num_agruments - 1; i ++)
            nested_arguments.push_back(arguments[i]);
        nested_func->setArguments(nested_arguments);
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
        if (static_cast<const ColumnUInt8 &>(*columns[num_agruments - 1]).getData()[row_num])
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

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        nested_func->insertResultInto(place, to);
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }

    bool isState() const override
    {
        return nested_func->isState();
    }

    static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena)
    {
        static_cast<const AggregateFunctionIf &>(*that).add(place, columns, row_num, arena);
    }

    IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }
};

}
