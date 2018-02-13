#pragma once

#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/** Not an aggregate function, but an adapter of aggregate functions,
  *  which any aggregate function `agg(x)` makes an aggregate function of the form `aggArray(x)`.
  * The adapted aggregate function calculates nested aggregate function for each element of the array.
  */
class AggregateFunctionArray final : public IAggregateFunctionHelper<AggregateFunctionArray>
{
private:
    AggregateFunctionPtr nested_func;
    size_t num_arguments;

public:
    AggregateFunctionArray(AggregateFunctionPtr nested_, const DataTypes & arguments)
        : nested_func(nested_), num_arguments(arguments.size())
    {
        for (const auto & type : arguments)
            if (!typeid_cast<const DataTypeArray *>(type.get()))
                throw Exception("All arguments for aggregate function " + getName() + " must be arrays", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override
    {
        return nested_func->getName() + "Array";
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
        const IColumn * nested[num_arguments];

        for (size_t i = 0; i < num_arguments; ++i)
            nested[i] = &static_cast<const ColumnArray &>(*columns[i]).getData();

        const ColumnArray & first_array_column = static_cast<const ColumnArray &>(*columns[0]);
        const IColumn::Offsets & offsets = first_array_column.getOffsets();

        size_t begin = row_num == 0 ? 0 : offsets[row_num - 1];
        size_t end = offsets[row_num];

        /// Sanity check. NOTE We can implement specialization for a case with single argument, if the check will hurt performance.
        for (size_t i = 1; i < num_arguments; ++i)
        {
            const ColumnArray & ith_column = static_cast<const ColumnArray &>(*columns[i]);
            const IColumn::Offsets & ith_offsets = ith_column.getOffsets();

            if (ith_offsets[row_num] != end || (row_num != 0 && ith_offsets[row_num - 1] != begin))
                throw Exception("Arrays passed to " + getName() + " aggregate function have different sizes", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
        }

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

    const char * getHeaderFilePath() const override { return __FILE__; }
};

}
