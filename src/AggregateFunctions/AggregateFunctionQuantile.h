#pragma once

#include <AggregateFunctions/FactoryHelpers.h>

/// These must be exposed in header for the purpose of dynamic compilation.
#include <AggregateFunctions/QuantileReservoirSampler.h>
#include <AggregateFunctions/QuantileReservoirSamplerDeterministic.h>
#include <AggregateFunctions/QuantileExact.h>
#include <AggregateFunctions/QuantileExactWeighted.h>
#include <AggregateFunctions/QuantileTiming.h>
#include <AggregateFunctions/QuantileTDigest.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/QuantilesCommon.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>

#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename> class QuantileTiming;


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
    /// If true, the function accepts the second argument
    /// (in can be "weight" to calculate quantiles or "determinator" that is used instead of PRNG).
    /// Second argument is always obtained through 'getUInt' method.
    bool has_second_arg,
    /// If non-void, the function will return float of specified type with possibly interpolated results and NaN if there was no values.
    /// Otherwise it will return Value type and default value if there was no values.
    /// As an example, the function cannot return floats, if the SQL type of argument is Date or DateTime.
    typename FloatReturnType,
    /// If true, the function will accept multiple parameters with quantile levels
    ///  and return an Array filled with many values of that quantiles.
    bool returns_many
>
class AggregateFunctionQuantile final : public IAggregateFunctionDataHelper<Data,
    AggregateFunctionQuantile<Value, Data, Name, has_second_arg, FloatReturnType, returns_many>>
{
private:
    using ColVecType = std::conditional_t<IsDecimalNumber<Value>, ColumnDecimal<Value>, ColumnVector<Value>>;

    static constexpr bool returns_float = !(std::is_same_v<FloatReturnType, void>);
    static_assert(!IsDecimalNumber<Value> || !returns_float);

    QuantileLevels<Float64> levels;

    /// Used when there are single level to get.
    Float64 level = 0.5;

    DataTypePtr & argument_type;

public:
    AggregateFunctionQuantile(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionQuantile<Value, Data, Name, has_second_arg, FloatReturnType, returns_many>>(argument_types_, params)
        , levels(params, returns_many), level(levels.levels[0]), argument_type(this->argument_types[0])
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
        auto value = static_cast<const ColVecType &>(*columns[0]).getData()[row_num];

        if constexpr (std::is_same_v<Data, QuantileTiming<Value>>)
        {
            /// QuantileTiming only supports integers.
            if (isNaN(value) || value > std::numeric_limits<Value>::max() || value < std::numeric_limits<Value>::min())
                return;
        }

        if constexpr (has_second_arg)
            this->data(place).add(
                value,
                columns[1]->getUInt(row_num));
        else
            this->data(place).add(value);
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

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        /// const_cast is required because some data structures apply finalizaton (like sorting) for obtain a result.
        auto & data = this->data(place);

        if constexpr (returns_many)
        {
            ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
            ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

            size_t size = levels.size();
            offsets_to.push_back(offsets_to.back() + size);

            if (!size)
                return;

            if constexpr (returns_float)
            {
                auto & data_to = assert_cast<ColumnVector<FloatReturnType> &>(arr_to.getData()).getData();
                size_t old_size = data_to.size();
                data_to.resize(data_to.size() + size);

                data.getManyFloat(levels.levels.data(), levels.permutation.data(), size, data_to.data() + old_size);
            }
            else
            {
                auto & data_to = static_cast<ColVecType &>(arr_to.getData()).getData();
                size_t old_size = data_to.size();
                data_to.resize(data_to.size() + size);

                data.getMany(levels.levels.data(), levels.permutation.data(), size, data_to.data() + old_size);
            }
        }
        else
        {
            if constexpr (returns_float)
                assert_cast<ColumnVector<FloatReturnType> &>(to).getData().push_back(data.getFloat(level));
            else
                static_cast<ColVecType &>(to).getData().push_back(data.get(level));
        }
    }

    static void assertSecondArg(const DataTypes & types)
    {
        if constexpr (has_second_arg)
        {
            assertBinary(Name::name, types);
            if (!isUnsignedInteger(types[1]))
                throw Exception("Second argument (weight) for function " + std::string(Name::name) + " must be unsigned integer, but it has type " + types[1]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
            assertUnary(Name::name, types);
    }
};

struct NameQuantile { static constexpr auto name = "quantile"; };
struct NameQuantiles { static constexpr auto name = "quantiles"; };
struct NameQuantileDeterministic { static constexpr auto name = "quantileDeterministic"; };
struct NameQuantilesDeterministic { static constexpr auto name = "quantilesDeterministic"; };

struct NameQuantileExact { static constexpr auto name = "quantileExact"; };
struct NameQuantilesExact { static constexpr auto name = "quantilesExact"; };

struct NameQuantileExactExclusive { static constexpr auto name = "quantileExactExclusive"; };
struct NameQuantilesExactExclusive { static constexpr auto name = "quantilesExactExclusive"; };

struct NameQuantileExactInclusive { static constexpr auto name = "quantileExactInclusive"; };
struct NameQuantilesExactInclusive { static constexpr auto name = "quantilesExactInclusive"; };

struct NameQuantileExactWeighted { static constexpr auto name = "quantileExactWeighted"; };
struct NameQuantilesExactWeighted { static constexpr auto name = "quantilesExactWeighted"; };

struct NameQuantileTiming { static constexpr auto name = "quantileTiming"; };
struct NameQuantileTimingWeighted { static constexpr auto name = "quantileTimingWeighted"; };
struct NameQuantilesTiming { static constexpr auto name = "quantilesTiming"; };
struct NameQuantilesTimingWeighted { static constexpr auto name = "quantilesTimingWeighted"; };

struct NameQuantileTDigest { static constexpr auto name = "quantileTDigest"; };
struct NameQuantileTDigestWeighted { static constexpr auto name = "quantileTDigestWeighted"; };
struct NameQuantilesTDigest { static constexpr auto name = "quantilesTDigest"; };
struct NameQuantilesTDigestWeighted { static constexpr auto name = "quantilesTDigestWeighted"; };

}
