#include <AggregateFunctions/AggregateFunctionExponentialTimeDecayed.h>

#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFloat.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <cmath>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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
    Float64 weighted_sum = 0;
    Float64 weight = 0;
    Float64 max_time = 0;
    bool initialized = false;

    void add(Float64 value, Float64 time, Float64 decay_length)
    {
        if (!initialized)
        {
            weighted_sum = value;
            weight = 1;
            max_time = time;
            initialized = true;
            return;
        }

        if (time > max_time)
        {
            const Float64 decay = std::exp((max_time - time) / decay_length);
            weighted_sum = weighted_sum * decay + value;
            weight = weight * decay + 1;
            max_time = time;
        }
        else
        {
            const Float64 decay = std::exp((time - max_time) / decay_length);
            weighted_sum += value * decay;
            weight += decay;
        }
    }

    void merge(const ExponentialTimeDecayedState & rhs, Float64 decay_length)
    {
        if (!rhs.initialized)
            return;

        if (!initialized)
        {
            *this = rhs;
            return;
        }

        const Float64 merged_max_time = std::max(max_time, rhs.max_time);
        const Float64 lhs_decay = std::exp((max_time - merged_max_time) / decay_length);
        const Float64 rhs_decay = std::exp((rhs.max_time - merged_max_time) / decay_length);

        weighted_sum = weighted_sum * lhs_decay + rhs.weighted_sum * rhs_decay;
        weight = weight * lhs_decay + rhs.weight * rhs_decay;
        max_time = merged_max_time;
    }

    void write(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(weighted_sum, buf);
        writeBinaryLittleEndian(weight, buf);
        writeBinaryLittleEndian(max_time, buf);
        writeBinaryLittleEndian(initialized, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinaryLittleEndian(weighted_sum, buf);
        readBinaryLittleEndian(weight, buf);
        readBinaryLittleEndian(max_time, buf);
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
    AggregateFunctionExponentialTimeDecayed(
        const DataTypes & argument_types,
        const Array & parameters,
        Float64 decay_length_)
        : IAggregateFunctionDataHelper<
              ExponentialTimeDecayedState,
              AggregateFunctionExponentialTimeDecayed<result_kind>>(
              argument_types, parameters, std::make_shared<DataTypeFloat64>())
        , decay_length(decay_length_)
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

        Float64 value = 1;
        if constexpr (result_kind != ExponentialTimeDecayedResult::Count)
            value = columns[0]->getFloat64(row_num);

        this->data(place).add(value, time, decay_length);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs), decay_length);
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
        Float64 result;

        if constexpr (result_kind == ExponentialTimeDecayedResult::Sum)
            result = state.weighted_sum;
        else if constexpr (result_kind == ExponentialTimeDecayedResult::Avg)
            result = state.weight > 0 ? state.weighted_sum / state.weight : std::numeric_limits<Float64>::quiet_NaN();
        else
            result = state.weight;

        assert_cast<ColumnFloat64 &>(to).getData().push_back(result);
    }

    bool allocatesMemoryInArena() const override { return false; }

private:
    const Float64 decay_length;
};

Float64 getDecayLength(const String & name, const Array & parameters)
{
    if (parameters.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} takes exactly one parameter, the decay length",
            name);

    const Float64 decay_length = applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[0]);
    if (!std::isfinite(decay_length) || decay_length <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Decay length of aggregate function {} must be finite and positive", name);

    return decay_length;
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
    const Settings *)
{
    assertValueAndTimeArguments(name, argument_types);
    return std::make_shared<AggregateFunctionExponentialTimeDecayed<ExponentialTimeDecayedResult::Sum>>(
        argument_types, parameters, getDecayLength(name, parameters));
}

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedAvg(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings *)
{
    assertValueAndTimeArguments(name, argument_types);
    return std::make_shared<AggregateFunctionExponentialTimeDecayed<ExponentialTimeDecayedResult::Avg>>(
        argument_types, parameters, getDecayLength(name, parameters));
}

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedCount(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings *)
{
    assertTimeArgument(name, argument_types);
    return std::make_shared<AggregateFunctionExponentialTimeDecayed<ExponentialTimeDecayedResult::Count>>(
        argument_types, parameters, getDecayLength(name, parameters));
}

}
