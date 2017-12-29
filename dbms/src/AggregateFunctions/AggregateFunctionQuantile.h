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

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** Generic aggregate function for calculation of quantiles.
  * It depends on quantile calculation data structure. Look at Quantile*.h for various implementations.
  */

template <
    /// Type of first argument.
    typename Value,
    /// Data structure and implementation of calculation. Look at QuantileExact.h for example.
    typename Data,
    /// Structure with static member "name", containing the name of the aggregate function.
    typename Name,
    /// If true, the function accept second argument
    /// (in can be "weight" to calculate quantiles or "determinator" that is used instead of PRNG).
    /// Second argument is always obtained through 'getUInt' method.
    bool have_second_arg,
    /// If non-void, the function will return float of specified type with possibly interpolated results and NaN if there was no values.
    /// Otherwise it will return Value type and default value if there was no values.
    /// As an example, the function cannot return floats, if the SQL type of argument is Date or DateTime.
    typename FloatReturnType,
    /// If true, the function will accept multiple parameters with quantile levels
    ///  and return an Array filled with many values of that quantiles.
    bool returns_many
>
class AggregateFunctionQuantile final : public IAggregateFunctionDataHelper<Data,
    AggregateFunctionQuantile<Value, Data, Name, have_second_arg, FloatReturnType, returns_many>>
{
private:
    static constexpr bool returns_float = !std::is_same_v<FloatReturnType, void>;

    QuantileLevels<Float64> levels;

    /// Used when there are single level to get.
    Float64 level = 0.5;

    DataTypePtr argument_type;

public:
    AggregateFunctionQuantile(const DataTypePtr & argument_type, const Array & params)
        : levels(params, returns_many), level(levels.levels[0]), argument_type(argument_type)
    {
        if (!returns_many && levels.size() > 1)
            throw Exception("Aggregate function " + getName() + " require one parameter or less", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    String getName() const override { return Name::name; }

    DataTypePtr getReturnType() const override
    {
        DataTypePtr res;

        if constexpr (returns_float)
            res = std::make_shared<DataTypeNumber<FloatReturnType>>();
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
            this->data(place).add(
                static_cast<const ColumnVector<Value> &>(*columns[0]).getData()[row_num],
                columns[1]->getUInt(row_num));
        else
            this->data(place).add(
                static_cast<const ColumnVector<Value> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        /// const_cast is required because some data structures apply finalizaton (like compactization) before serializing.
        this->data(const_cast<AggregateDataPtr>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        /// const_cast is required because some data structures apply finalizaton (like sorting) for obtain a result.
        auto & data = this->data(const_cast<AggregateDataPtr>(place));

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
                auto & data_to = static_cast<ColumnVector<FloatReturnType> &>(arr_to.getData()).getData();
                size_t old_size = data_to.size();
                data_to.resize(data_to.size() + size);

                data.getManyFloat(&levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
            }
            else
            {
                auto & data_to = static_cast<ColumnVector<Value> &>(arr_to.getData()).getData();
                size_t old_size = data_to.size();
                data_to.resize(data_to.size() + size);

                data.getMany(&levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
            }
        }
        else
        {
            if constexpr (returns_float)
                static_cast<ColumnVector<FloatReturnType> &>(to).getData().push_back(data.getFloat(level));
            else
                static_cast<ColumnVector<Value> &>(to).getData().push_back(data.get(level));
        }
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

}
