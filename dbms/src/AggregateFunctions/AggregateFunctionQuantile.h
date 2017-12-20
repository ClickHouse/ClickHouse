#pragma once

#include <type_traits>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/QuantilesCommon.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

template <
    typename Value,
    typename SecondArg,
    typename Data,
    typename Name,
    bool returns_float,
    bool returns_many
>
class AggregateFunctionQuantile final : public IAggregateFunctionDataHelper<Data,
    AggregateFunctionQuantile<Value, SecondArg, Data, Name, returns_float, returns_many>>
{
private:
    bool have_second_arg = !std::is_same_v<SecondArg, void>;

    Float64 level = 0.5;
    QuantileLevels levels;
    DataTypePtr argument_type;

public:
    AggregateFunctionQuantile(const DataTypePtr & argument_type, double level)
        : level(level), argument_type(argument_type) {}

    AggregateFunctionQuantile(const DataTypePtr & argument_type, const Array & params)
        : levels(params), argument_type(argument_type) {}

    String getName() const override { return Name::name; }

    DataTypePtr getReturnType() const override
    {
        DataTypePtr res;

        if constexpr (returns_float)
            res = std::make_shared<DataTypeFloat32>();
        else
            res = argument_type;

        if constexpr (returns_many)
            return std::make_shared<DataTypeArray>(res);
        else
            return res;
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (have_second_arg)
            this->data(place).insert(
                static_cast<const ColumnVector<Value> &>(*columns[0]).getData()[row_num],
                static_cast<const ColumnVector<SecondArg> &>(*columns[1]).getData()[row_num]);
        else if constexpr (have_determinator)

        else
            this->data(place).insert(
                static_cast<const ColumnVector<Value> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        if constexpr (returns_many)
        {
            ColumnArray & arr_to = static_cast<ColumnArray &>(to);
            ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

            size_t size = levels.size();
            offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

            if (!size)
                return;

            if constexpr (returns_float)
            {
                typename ColumnFloat32::Container & data_to = static_cast<ColumnFloat32 &>(arr_to.getData()).getData();
                size_t old_size = data_to.size();
                data_to.resize(data_to.size() + size);

                this->data(place).getManyFloat(&levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
            }
            else
            {
                typename ColumnVector<Value>::Container & data_to = static_cast<ColumnVector<Value> &>(arr_to.getData()).getData();
                size_t old_size = data_to.size();
                data_to.resize(data_to.size() + size);

                this->data(place).getMany(&levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
            }
        }
        else
        {
            if constexpr (returns_float)
                static_cast<ColumnFloat32 &>(to).getData().push_back(this->data(place).getFloat(level));
            else
                static_cast<ColumnVector<Value> &>(to).getData().push_back(this->data(place).get(level));
        }
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

}
