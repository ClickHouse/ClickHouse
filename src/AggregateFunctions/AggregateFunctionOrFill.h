#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
}

/**
  * -OrDefault and -OrNull combinators for aggregate functions.
  * If there are no input values, return NULL or a default value, accordingly.
  * Use a single additional byte of data after the nested function data:
  * 0 means there was no input, 1 means there was some.
  */
template <bool UseNull>
class AggregateFunctionOrFill final : public IAggregateFunctionHelper<AggregateFunctionOrFill<UseNull>>
{
private:
    AggregateFunctionPtr nested_function;

    size_t size_of_data;
    DataTypePtr inner_type;
    bool inner_nullable;

public:
    AggregateFunctionOrFill(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionOrFill>{arguments, params}
        , nested_function{nested_function_}
        , size_of_data {nested_function->sizeOfData()}
        , inner_type {nested_function->getReturnType()}
        , inner_nullable {inner_type->isNullable()}
    {
        // nothing
    }

    String getName() const override
    {
        if constexpr (UseNull)
            return nested_function->getName() + "OrNull";
        else
            return nested_function->getName() + "OrDefault";
    }

    bool isState() const override
    {
        return nested_function->isState();
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena();
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return size_of_data + sizeof(char);
    }

    size_t alignOfData() const override
    {
        return nested_function->alignOfData();
    }

    void create(AggregateDataPtr place) const override
    {
        nested_function->create(place);

        place[size_of_data] = 0;
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        nested_function->destroy(place);
    }

    void add(
        AggregateDataPtr place,
        const IColumn ** columns,
        size_t row_num,
        Arena * arena) const override
    {
        nested_function->add(place, columns, row_num, arena);

        place[size_of_data] = 1;
    }

    void merge(
        AggregateDataPtr place,
        ConstAggregateDataPtr rhs,
        Arena * arena) const override
    {
        nested_function->merge(place, rhs, arena);

        place[size_of_data] |= rhs[size_of_data];
    }

    void serialize(
        ConstAggregateDataPtr place,
        WriteBuffer & buf) const override
    {
        nested_function->serialize(place, buf);

        writeChar(place[size_of_data], buf);
    }

    void deserialize(
        AggregateDataPtr place,
        ReadBuffer & buf,
        Arena * arena) const override
    {
        nested_function->deserialize(place, buf, arena);

        readChar(place[size_of_data], buf);
    }

    DataTypePtr getReturnType() const override
    {
        if constexpr (UseNull)
        {
            // -OrNull

            if (inner_nullable)
                return inner_type;

            return std::make_shared<DataTypeNullable>(inner_type);
        }
        else
        {
            // -OrDefault

            return inner_type;
        }
    }

    void insertResultInto(
        AggregateDataPtr place,
        IColumn & to,
        Arena * arena) const override
    {
        if (place[size_of_data])
        {
            if constexpr (UseNull)
            {
                // -OrNull

                if (inner_nullable)
                    nested_function->insertResultInto(place, to, arena);
                else
                {
                    ColumnNullable & col = typeid_cast<ColumnNullable &>(to);

                    col.getNullMapColumn().insertDefault();
                    nested_function->insertResultInto(place, col.getNestedColumn(), arena);
                }
            }
            else
            {
                // -OrDefault

                nested_function->insertResultInto(place, to, arena);
            }
        }
        else
            to.insertDefault();
    }
};

}
