#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** Not an aggregate function, but an adapter of aggregate functions,
  * which any aggregate function `agg(x)` makes an aggregate function of the form `aggIf(x, cond)`.
  * The adapted aggregate function takes two arguments - a value and a condition,
  * and calculates the nested aggregate function for the values when the condition is satisfied.
  * For example, avgIf(x, cond) calculates the average x if `cond`.
  */
class AggregateFunctionIf final : public IAggregateFunctionHelper<AggregateFunctionIf>
{
private:
    AggregateFunctionPtr nested_func;
    size_t num_arguments;

public:
    AggregateFunctionIf(AggregateFunctionPtr nested, const DataTypes & types)
        : IAggregateFunctionHelper<AggregateFunctionIf>(types, nested->getParameters())
        , nested_func(nested), num_arguments(types.size())
    {
        if (num_arguments == 0)
            throw Exception("Aggregate function " + getName() + " require at least one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isUInt8(types.back()))
            throw Exception("Last argument for aggregate function " + getName() + " must be UInt8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override
    {
        return nested_func->getName() + "If";
    }

    DataTypePtr getReturnType() const override
    {
        return nested_func->getReturnType();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        nested_func->create(place);
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
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

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (assert_cast<const ColumnUInt8 &>(*columns[num_arguments - 1]).getData()[row_num])
            nested_func->add(place, columns, row_num, arena);
    }

    void addBatch(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t) const override
    {
        nested_func->addBatch(batch_size, places, place_offset, columns, arena, num_arguments - 1);
    }

    void addBatchSinglePlace(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t) const override
    {
        nested_func->addBatchSinglePlace(batch_size, place, columns, arena, num_arguments - 1);
    }

    void addBatchSinglePlaceNotNull(
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t) const override
    {
        nested_func->addBatchSinglePlaceNotNull(batch_size, place, columns, null_map, arena, num_arguments - 1);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_func->merge(place, rhs, arena);
    }

    void mergeBatch(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const AggregateDataPtr * rhs,
        Arena * arena) const override
    {
        nested_func->mergeBatch(batch_size, places, place_offset, rhs, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        nested_func->serialize(place, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        nested_func->deserialize(place, buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertResultInto(place, to, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }

    bool isState() const override
    {
        return nested_func->isState();
    }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments,
        const Array & params, const AggregateFunctionProperties & properties) const override;

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

}
