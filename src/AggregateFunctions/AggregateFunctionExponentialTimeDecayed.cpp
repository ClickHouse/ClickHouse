#include <AggregateFunctions/AggregateFunctionExponentialTimeDecayed.h>

#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeExponentialTimeDecayingFloat64.h>
#include <DataTypes/DataTypeFloat.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <cmath>
#include <limits>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_time_decay_aggregate_functions;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

namespace
{

enum class ExponentialTimeDecayedResult
{
    Sum,
    Avg,
    Count,
};

struct ExponentialTimeDecayedState
{
    /// Every state is evaluated at max_time:
    /// weighted_sum = sum(value_i * exp2((time_i - max_time) / half_life))
    /// weight = sum(exp2((time_i - max_time) / half_life))
    /// This representation makes merging states the same operation as adding rows,
    /// independent of row order, batch distribution, and batch count.
    Float64 weighted_sum = 0;
    Float64 weight = 0;
    Float64 max_time = 0;
    Field max_time_field;
    bool initialized = false;

    void add(Float64 value, Float64 time, Field time_field, Float64 half_life)
    {
        if (!initialized)
        {
            weighted_sum = value;
            weight = 1;
            max_time = time;
            max_time_field = std::move(time_field);
            initialized = true;
            return;
        }

        if (time_field > max_time_field)
        {
            const Float64 decay = std::exp2((max_time - time) / half_life);
            weighted_sum = weighted_sum * decay + value;
            weight = weight * decay + 1;
            max_time = time;
            max_time_field = std::move(time_field);
        }
        else
        {
            const Float64 decay = std::exp2((time - max_time) / half_life);
            weighted_sum += value * decay;
            weight += decay;
        }
    }

    void merge(const ExponentialTimeDecayedState & rhs, Float64 half_life)
    {
        if (!rhs.initialized)
            return;

        if (!initialized)
        {
            *this = rhs;
            return;
        }

        /// Re-anchor both states at their shared greatest timestamp before adding them.
        const bool rhs_is_latest = rhs.max_time_field > max_time_field;
        const Float64 merged_max_time = rhs_is_latest ? rhs.max_time : max_time;
        const Float64 lhs_decay = std::exp2((max_time - merged_max_time) / half_life);
        const Float64 rhs_decay = std::exp2((rhs.max_time - merged_max_time) / half_life);

        weighted_sum = weighted_sum * lhs_decay + rhs.weighted_sum * rhs_decay;
        weight = weight * lhs_decay + rhs.weight * rhs_decay;
        max_time = merged_max_time;
        if (rhs_is_latest)
            max_time_field = rhs.max_time_field;
    }

    void write(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(weighted_sum, buf);
        writeBinaryLittleEndian(weight, buf);
        writeBinaryLittleEndian(max_time, buf);
        writeFieldBinary(max_time_field, buf);
        writeBinaryLittleEndian(initialized, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinaryLittleEndian(weighted_sum, buf);
        readBinaryLittleEndian(weight, buf);
        readBinaryLittleEndian(max_time, buf);
        max_time_field = readFieldBinary(buf);
        readBinaryLittleEndian(initialized, buf);
    }
};

template <ExponentialTimeDecayedResult result_kind>
class AggregateFunctionExponentialTimeDecayed final
    : public IAggregateFunctionDataHelper<
          ExponentialTimeDecayedState,
          AggregateFunctionExponentialTimeDecayed<result_kind>>
{
public:
    static DataTypePtr getResultType(const DataTypes & argument_types)
    {
        if constexpr (result_kind == ExponentialTimeDecayedResult::Avg)
            return std::make_shared<DataTypeFloat64>();

        const size_t time_argument = result_kind == ExponentialTimeDecayedResult::Count ? 0 : 1;
        return createDataTypeExponentialTimeDecayingFloat64(argument_types[time_argument]);
    }

    AggregateFunctionExponentialTimeDecayed(
        const DataTypes & argument_types,
        const Array & parameters,
        Float64 half_life_)
        : IAggregateFunctionDataHelper<
              ExponentialTimeDecayedState,
              AggregateFunctionExponentialTimeDecayed<result_kind>>(
              argument_types, parameters, getResultType(argument_types))
        , half_life(half_life_)
        , time_type(argument_types[result_kind == ExponentialTimeDecayedResult::Count ? 0 : 1])
    {
    }

    String getName() const override
    {
        if constexpr (result_kind == ExponentialTimeDecayedResult::Sum)
            return "exponentialTimeDecayedSum";
        if constexpr (result_kind == ExponentialTimeDecayedResult::Avg)
            return "exponentialTimeDecayedAvg";
        return "exponentialTimeDecayedCount";
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const size_t time_argument = result_kind == ExponentialTimeDecayedResult::Count ? 0 : 1;
        const Float64 time = columns[time_argument]->getFloat64(row_num);
        if (!std::isfinite(time))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time of aggregate function {} must be finite", getName());
        Field time_field;
        columns[time_argument]->get(row_num, time_field);

        Float64 value = 1;
        if constexpr (result_kind != ExponentialTimeDecayedResult::Count)
        {
            value = columns[0]->getFloat64(row_num);
            if (!std::isfinite(value))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value of aggregate function {} must be finite", getName());
        }
        if constexpr (result_kind == ExponentialTimeDecayedResult::Sum)
        {
            if (value < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value of aggregate function {} must be non-negative", getName());
        }

        this->data(place).add(value, time, std::move(time_field), half_life);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs), half_life);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & state = this->data(place);
        if constexpr (result_kind == ExponentialTimeDecayedResult::Avg)
        {
            const Float64 result = state.weight > 0
                ? state.weighted_sum / state.weight
                : std::numeric_limits<Float64>::quiet_NaN();
            assert_cast<ColumnFloat64 &>(to).getData().push_back(result);
        }
        else
        {
            const Float64 result = result_kind == ExponentialTimeDecayedResult::Sum
                ? state.weighted_sum
                : state.weight;
            Tuple decaying_value{
                Field(result),
                state.initialized ? state.max_time_field : time_type->getDefault(),
                Field(half_life)};
            to.insert(Field(decaying_value));
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

private:
    const Float64 half_life;
    const DataTypePtr time_type;
};

Float64 getHalfLife(const String & name, const Array & parameters)
{
    if (parameters.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} takes exactly one parameter, the half-life",
            name);

    const Float64 half_life = applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[0]);
    if (!std::isfinite(half_life) || half_life <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Half-life of aggregate function {} must be finite and positive", name);

    return half_life;
}

void assertExperimentalFeatureEnabled(const String & name, const Settings * settings)
{
    if (settings && !(*settings)[Setting::allow_experimental_time_decay_aggregate_functions])
        throw Exception(
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
            "Aggregate function {} is experimental and disabled by default. Enable it with setting "
            "allow_experimental_time_decay_aggregate_functions",
            name);
}

void assertValueAndTimeArguments(const String & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} takes exactly two arguments", name);

    if (!isNumber(argument_types[0]))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument of aggregate function {} must be a number, got {}",
            name,
            argument_types[0]->getName());

    if (!isNumber(argument_types[1]) && !isDateTime(argument_types[1]) && !isDateTime64(argument_types[1]))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument of aggregate function {} must be a number, DateTime, or DateTime64, got {}",
            name,
            argument_types[1]->getName());
}

void assertTimeArgument(const String & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} takes exactly one argument", name);

    if (!isNumber(argument_types[0]) && !isDateTime(argument_types[0]) && !isDateTime64(argument_types[0]))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of aggregate function {} must be a number, DateTime, or DateTime64, got {}",
            name,
            argument_types[0]->getName());
}

}

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedSum(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings * settings)
{
    assertExperimentalFeatureEnabled(name, settings);
    assertValueAndTimeArguments(name, argument_types);
    return std::make_shared<AggregateFunctionExponentialTimeDecayed<ExponentialTimeDecayedResult::Sum>>(
        argument_types, parameters, getHalfLife(name, parameters));
}

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedAvg(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings * settings)
{
    assertExperimentalFeatureEnabled(name, settings);
    assertValueAndTimeArguments(name, argument_types);
    return std::make_shared<AggregateFunctionExponentialTimeDecayed<ExponentialTimeDecayedResult::Avg>>(
        argument_types, parameters, getHalfLife(name, parameters));
}

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedCount(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings * settings)
{
    assertExperimentalFeatureEnabled(name, settings);
    assertTimeArgument(name, argument_types);
    return std::make_shared<AggregateFunctionExponentialTimeDecayed<ExponentialTimeDecayedResult::Count>>(
        argument_types, parameters, getHalfLife(name, parameters));
}

}
