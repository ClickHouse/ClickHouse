#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>


namespace DB
{

template <typename Key>
class AggregateFunctionResample final : public IAggregateFunctionHelper<
    AggregateFunctionResample<Key>
>
{
private:
    AggregateFunctionPtr nested_function;

    size_t last_col;

    Key begin;
    Key end;
    Key step;

    size_t total;
    size_t aod;
    size_t sod;

public:
    AggregateFunctionResample(
        AggregateFunctionPtr nested_function,
        Key begin,
        Key end,
        Key step,
        const DataTypes & arguments,
        const Array & params
    ) :
        IAggregateFunctionHelper<
            AggregateFunctionResample<Key>
        > {arguments, params},
        nested_function {nested_function},
        last_col {arguments.size() - 1},
        begin {begin},
        end {end},
        step {step},
        total {
            static_cast<size_t>(
                (end - begin + step - (step >= 0 ? 1 : -1)) / step
            )
        },
        aod {nested_function->alignOfData()},
        sod {(nested_function->sizeOfData() + aod - 1) / aod * aod}
    {
        // notice: argument types has been checked before
    }

    String getName() const override
    {
        return nested_function->getName() + "Resample";
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
        return total * sod;
    }

    size_t alignOfData() const override
    {
        return aod;
    }

    void create(AggregateDataPtr place) const override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->create(place + i * sod);
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->destroy(place + i * sod);
    }

    void add(
        AggregateDataPtr place,
        const IColumn ** columns,
        size_t row_num,
        Arena * arena
    ) const override
    {
        // Key key {
        //     static_cast<const ColumnVector<Key> *>(
        //         columns[last_col]
        //     )->getData()[row_num]
        // };
        Key key;

        if constexpr (static_cast<Key>(-1) < 0)
            key = columns[last_col]->getInt(row_num);
        else
            key = columns[last_col]->getUInt(row_num);

        size_t pos = (key - begin) / step;

        if (pos >= 0 && pos < total)
            nested_function->add(place + pos * sod, columns, row_num, arena);
    }

    void merge(
        AggregateDataPtr place,
        ConstAggregateDataPtr rhs,
        Arena * arena
    ) const override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->merge(place + i * sod, rhs + i * sod, arena);
    }

    void serialize(
        ConstAggregateDataPtr place,
        WriteBuffer & buf
    ) const override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->serialize(place + i * sod, buf);
    }

    void deserialize(
        AggregateDataPtr place,
        ReadBuffer & buf,
        Arena * arena
    ) const override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->deserialize(place + i * sod, buf, arena);
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(
            nested_function->getReturnType()
        );
    }

    void insertResultInto(
        ConstAggregateDataPtr place,
        IColumn & to
    ) const override
    {
        auto & col = static_cast<ColumnArray &>(to);
        auto & col_offsets = static_cast<ColumnArray::ColumnOffsets &>(
            col.getOffsetsColumn()
        );

        for (size_t i = 0; i < total; ++i)
            nested_function->insertResultInto(place + i * sod, col.getData());

        col_offsets.getData().push_back(col.getData().size());
    }
};

}
