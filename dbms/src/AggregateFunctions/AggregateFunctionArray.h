#pragma once

#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/WriteHelpers.h>


namespace DB
{


/** Not an aggregate function, but an adapter of aggregate functions,
  *  which any aggregate function `agg(x)` makes an aggregate function of the form `aggArray(x)`.
  * The adapted aggregate function calculates nested aggregate function for each element of the array.
  */
class AggregateFunctionArray final : public IAggregateFunction
{
private:
    AggregateFunctionPtr nested_func_owner;
    IAggregateFunction * nested_func;
    size_t num_agruments;

public:
    AggregateFunctionArray(AggregateFunctionPtr nested_) : nested_func_owner(nested_), nested_func(nested_func_owner.get()) {}

    String getName() const override
    {
        return nested_func->getName() + "Array";
    }

    DataTypePtr getReturnType() const override
    {
        return nested_func->getReturnType();
    }

    void setArguments(const DataTypes & arguments) override
    {
        num_agruments = arguments.size();

        if (0 == num_agruments)
            throw Exception("Array aggregate functions requires at least one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes nested_arguments;
        for (size_t i = 0; i < num_agruments; ++i)
        {
            if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(&*arguments[i]))
                nested_arguments.push_back(array->getNestedType());
            else
                throw Exception("Illegal type " + arguments[i]->getName() + " of argument #" + toString(i + 1) + " for aggregate function " + getName() + ". Must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

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
        const IColumn * nested[num_agruments];

        for (size_t i = 0; i < num_agruments; ++i)
            nested[i] = &static_cast<const ColumnArray &>(*columns[i]).getData();

        const ColumnArray & first_array_column = static_cast<const ColumnArray &>(*columns[0]);
        const IColumn::Offsets_t & offsets = first_array_column.getOffsets();

        size_t begin = row_num == 0 ? 0 : offsets[row_num - 1];
        size_t end = offsets[row_num];

        for (size_t i = begin; i < end; ++i)
            nested_func->add(place, nested, i, arena);
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
        static_cast<const AggregateFunctionArray &>(*that).add(place, columns, row_num, arena);
    }

    IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }
};

}
