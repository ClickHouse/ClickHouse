#pragma once

#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/QuantilesCommon.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>
#include <Interpreters/GatherFunctionQuantileVisitor.h>

#include <type_traits>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

template <typename> class QuantileTiming;
template <typename> class QuantileGK;
template <typename> class QuantileDD;

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
    bool returns_many,
    /// If the first parameter (before level) is accuracy.
    bool has_accuracy_parameter>
class AggregateFunctionQuantile final
    : public IAggregateFunctionDataHelper<Data, AggregateFunctionQuantile<Value, Data, Name, has_second_arg, FloatReturnType, returns_many, has_accuracy_parameter>>
{
private:
    using ColVecType = ColumnVectorOrDecimal<Value>;

    static constexpr bool returns_float = !(std::is_same_v<FloatReturnType, void>);
    static constexpr bool is_quantile_ddsketch = std::is_same_v<Data, QuantileDD<Value>>;
    static_assert(!is_decimal<Value> || !returns_float);

    QuantileLevels<Float64> levels;

    /// Used when there are single level to get.
    Float64 level = 0.5;

    /// Used for the approximate version of the algorithm (Greenwald-Khanna)
    ssize_t accuracy = 10000;

    /// Used for the quantile sketch
    Float64 relative_accuracy = 0.01;

    DataTypePtr & argument_type;

public:
    AggregateFunctionQuantile(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionQuantile<Value, Data, Name, has_second_arg, FloatReturnType, returns_many, has_accuracy_parameter>>(
            argument_types_, params, createResultType(argument_types_))
        , levels(has_accuracy_parameter && !params.empty() ? Array(params.begin() + 1, params.end()) : params, returns_many)
        , level(levels.levels[0])
        , argument_type(this->argument_types[0])
    {
        if (!returns_many && levels.size() > 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires one level parameter or less", getName());

        if constexpr (has_second_arg)
        {
            assertBinary(Name::name, argument_types_);
            if (!isUInt(argument_types_[1]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument (weight) for function {} must be unsigned integer, but it has type {}",
                    Name::name,
                    argument_types_[1]->getName());
        }
        else
        {
            assertUnary(Name::name, argument_types_);
        }

        if constexpr (is_quantile_ddsketch)
        {
            if (params.empty())
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires at least one param", getName());

            const auto & relative_accuracy_field = params[0];
            if (relative_accuracy_field.getType() != Field::Types::Float64)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} requires relative accuracy parameter with Float64 type", getName());

            relative_accuracy = relative_accuracy_field.safeGet<Float64>();

            if (relative_accuracy <= 0 || relative_accuracy >= 1 || isNaN(relative_accuracy))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function {} requires relative accuracy parameter with value between 0 and 1 but is {}",
                    getName(),
                    relative_accuracy);
            // Throw exception if the relative accuracy is too small.
            // This is to avoid the case where the user specifies a relative accuracy that is too small
            // and the sketch is not able to allocate enough memory to satisfy the accuracy requirement.
            if (relative_accuracy < 1e-6)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function {} requires relative accuracy parameter with value greater than 1e-6 but is {}",
                    getName(),
                    relative_accuracy);
        }
        else if constexpr (has_accuracy_parameter)
        {
            if (params.empty())
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires at least one param", getName());

            const auto & accuracy_field = params[0];
            if (!isInt64OrUInt64FieldType(accuracy_field.getType()))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} requires accuracy parameter with integer type", getName());

            if (accuracy_field.getType() == Field::Types::Int64)
                accuracy = accuracy_field.safeGet<Int64>();
            else
                accuracy = accuracy_field.safeGet<UInt64>();

            if (accuracy <= 0)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function {} requires accuracy parameter with positive value but is {}",
                    getName(),
                    accuracy);
        }
    }

    String getName() const override { return Name::name; }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        if constexpr (is_quantile_ddsketch)
            new (place) Data(relative_accuracy);
        else if constexpr (has_accuracy_parameter)
            new (place) Data(accuracy);
        else
            new (place) Data;
    }

    static DataTypePtr createResultType(const DataTypes & argument_types_)
    {
        DataTypePtr res;

        if constexpr (returns_float)
            res = std::make_shared<DataTypeNumber<FloatReturnType>>();
        else
            res = argument_types_[0];

        if constexpr (returns_many)
            return std::make_shared<DataTypeArray>(res);
        else
            return res;
    }

    bool haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const override
    {
        return GatherFunctionQuantileData::toFusedNameOrSelf(getName()) == GatherFunctionQuantileData::toFusedNameOrSelf(rhs.getName())
            && this->haveEqualArgumentTypes(rhs);
    }

    DataTypePtr getNormalizedStateType() const override
    {
        /// Return normalized state type: quantiles*(1)(...)
        Array params{1};
        if constexpr (is_quantile_ddsketch)
            params = {relative_accuracy, 1};
        else if constexpr (has_accuracy_parameter)
            params = {accuracy, 1};
        AggregateFunctionProperties properties;
        return std::make_shared<DataTypeAggregateFunction>(
            AggregateFunctionFactory::instance().get(
                GatherFunctionQuantileData::toFusedNameOrSelf(getName()), NullsAction::EMPTY, this->argument_types, params, properties),
            this->argument_types,
            params);
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto value = static_cast<const ColVecType &>(*columns[0]).getData()[row_num];

        if constexpr (std::is_same_v<Data, QuantileTiming<Value>>)
        {
            /// QuantileTiming only supports unsigned integers. Too large values are also meaningless.
#   pragma clang diagnostic push
#   pragma clang diagnostic ignored "-Wimplicit-const-int-float-conversion"
            if (isNaN(value) || value > std::numeric_limits<Int64>::max() || value < 0)
                return;
#   pragma clang diagnostic pop
        }

        if constexpr (has_second_arg)
            this->data(place).add(value, columns[1]->getUInt(row_num));
        else
            this->data(place).add(value);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        /// const_cast is required because some data structures apply finalizaton (like compactization) before serializing.
        this->data(const_cast<AggregateDataPtr>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
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
                data_to.resize(old_size + size);

                data.getManyFloat(levels.levels.data(), levels.permutation.data(), size, data_to.data() + old_size);
            }
            else
            {
                auto & data_to = static_cast<ColVecType &>(arr_to.getData()).getData();
                size_t old_size = data_to.size();
                data_to.resize(old_size + size);

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
};

struct NameQuantile { static constexpr auto name = "quantile"; };
struct NameQuantiles { static constexpr auto name = "quantiles"; };
struct NameQuantileDeterministic { static constexpr auto name = "quantileDeterministic"; };
struct NameQuantilesDeterministic { static constexpr auto name = "quantilesDeterministic"; };

struct NameQuantileExact { static constexpr auto name = "quantileExact"; };
struct NameQuantilesExact { static constexpr auto name = "quantilesExact"; };

struct NameQuantileExactLow { static constexpr auto name = "quantileExactLow"; };
struct NameQuantilesExactLow { static constexpr auto name = "quantilesExactLow"; };

struct NameQuantileExactHigh { static constexpr auto name = "quantileExactHigh"; };
struct NameQuantilesExactHigh { static constexpr auto name = "quantilesExactHigh"; };

struct NameQuantileExactExclusive { static constexpr auto name = "quantileExactExclusive"; };
struct NameQuantilesExactExclusive { static constexpr auto name = "quantilesExactExclusive"; };

struct NameQuantileExactInclusive { static constexpr auto name = "quantileExactInclusive"; };
struct NameQuantilesExactInclusive { static constexpr auto name = "quantilesExactInclusive"; };

struct NameQuantileExactWeighted { static constexpr auto name = "quantileExactWeighted"; };
struct NameQuantilesExactWeighted { static constexpr auto name = "quantilesExactWeighted"; };

struct NameQuantileInterpolatedWeighted { static constexpr auto name = "quantileInterpolatedWeighted"; };
struct NameQuantilesInterpolatedWeighted { static constexpr auto name = "quantilesInterpolatedWeighted"; };

struct NameQuantileTiming { static constexpr auto name = "quantileTiming"; };
struct NameQuantileTimingWeighted { static constexpr auto name = "quantileTimingWeighted"; };
struct NameQuantilesTiming { static constexpr auto name = "quantilesTiming"; };
struct NameQuantilesTimingWeighted { static constexpr auto name = "quantilesTimingWeighted"; };

struct NameQuantileTDigest { static constexpr auto name = "quantileTDigest"; };
struct NameQuantileTDigestWeighted { static constexpr auto name = "quantileTDigestWeighted"; };
struct NameQuantilesTDigest { static constexpr auto name = "quantilesTDigest"; };
struct NameQuantilesTDigestWeighted { static constexpr auto name = "quantilesTDigestWeighted"; };

struct NameQuantileBFloat16 { static constexpr auto name = "quantileBFloat16"; };
struct NameQuantilesBFloat16 { static constexpr auto name = "quantilesBFloat16"; };
struct NameQuantileBFloat16Weighted { static constexpr auto name = "quantileBFloat16Weighted"; };
struct NameQuantilesBFloat16Weighted { static constexpr auto name = "quantilesBFloat16Weighted"; };

struct NameQuantileGK { static constexpr auto name = "quantileGK"; };
struct NameQuantilesGK { static constexpr auto name = "quantilesGK"; };

struct NameQuantileDD { static constexpr auto name = "quantileDD"; };
struct NameQuantilesDD { static constexpr auto name = "quantilesDD"; };

}
