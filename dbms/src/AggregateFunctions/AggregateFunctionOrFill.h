#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

template <bool UseNull>
class AggregateFunctionOrFill final : public IAggregateFunctionHelper<AggregateFunctionOrFill<UseNull>>
{
private:
    AggregateFunctionPtr nested_function;

    size_t sod;
    DataTypePtr inner_type;
    bool inner_nullable;

public:
    AggregateFunctionOrFill(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionOrFill>{arguments, params}
        , nested_function{nested_function_}
        , sod {nested_function->sizeOfData()}
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

    const char * getHeaderFilePath() const override
    {
        return __FILE__;
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
        return sod + sizeof(bool);
    }

    size_t alignOfData() const override
    {
        return nested_function->alignOfData();
    }

    void create(AggregateDataPtr place) const override
    {
        nested_function->create(place);

        place[sod] = 0;
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

        place[sod] = 1;
    }

    void merge(
        AggregateDataPtr place,
        ConstAggregateDataPtr rhs,
        Arena * arena) const override
    {
        nested_function->merge(place, rhs, arena);
    }

    void serialize(
        ConstAggregateDataPtr place,
        WriteBuffer & buf) const override
    {
        nested_function->serialize(place, buf);
    }

    void deserialize(
        AggregateDataPtr place,
        ReadBuffer & buf,
        Arena * arena) const override
    {
        nested_function->deserialize(place, buf, arena);
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
        ConstAggregateDataPtr place,
        IColumn & to) const override
    {
        if constexpr (UseNull)
        {
            // -OrNull

            ColumnNullable & col = static_cast<ColumnNullable &>(to);

            if (place[sod])
            {
                if (inner_nullable)
                    nested_function->insertResultInto(place, col);
                else
                    nested_function->insertResultInto(place, col.getNestedColumn());
            }
            else
                col.insertDefault();
        }
        else
        {
            // -OrDefault

            if (place[sod])
                nested_function->insertResultInto(place, to);
            else
                to.insertDefault();
        }
    }
};

}
