#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionExponentialTimeDecayed.h>
#include <DataTypes/DataTypeFloat.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedSum(
        const std::string & name,
        const DataTypes & argument_types,
        const Array & parameters,
        const Settings * settings)
    {
        if (argument_types.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly 2 arguments", name);

        if (!isNumber(argument_types[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of {} must be a number", name);

        if (!isNumber(argument_types[1]) && !isDateTime(argument_types[1]) && !isDateTime64(argument_types[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of {} must be a number or DateTime type", name);

        Float64 decay_length = AggregateFunctionExponentialTimeDecayedSum<Float64>::getDecayLength(parameters);

        return std::make_shared<AggregateFunctionExponentialTimeDecayedSum<Float64>>(argument_types, parameters, decay_length);
    }

    AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedAvg(
        const std::string & name,
        const DataTypes & argument_types,
        const Array & parameters,
        const Settings * settings)
    {
        if (argument_types.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly 2 arguments", name);

        if (!isNumber(argument_types[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of {} must be a number", name);

        if (!isNumber(argument_types[1]) && !isDateTime(argument_types[1]) && !isDateTime64(argument_types[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of {} must be a number or DateTime type", name);

        Float64 decay_length = AggregateFunctionExponentialTimeDecayedAvg<Float64>::getDecayLength(parameters);

        return std::make_shared<AggregateFunctionExponentialTimeDecayedAvg<Float64>>(argument_types, parameters, decay_length);
    }

    AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedCount(
        const std::string & name,
        const DataTypes & argument_types,
        const Array & parameters,
        const Settings * settings)
    {
        if (argument_types.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly 1 argument", name);

        if (!isNumber(argument_types[0]) && !isDateTime(argument_types[0]) && !isDateTime64(argument_types[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of {} must be a number or DateTime type", name);

        Float64 decay_length = AggregateFunctionExponentialTimeDecayedCount<Float64>::getDecayLength(parameters);

        return std::make_shared<AggregateFunctionExponentialTimeDecayedCount<Float64>>(argument_types, parameters, decay_length);
    }
}

void registerAggregateFunctionExponentialTimeDecayed(AggregateFunctionFactory & factory)
{
    factory.registerFunction("exponentialTimeDecayedSum",
        {createAggregateFunctionExponentialTimeDecayedSum, {}, {.is_state = true}});

    factory.registerFunction("exponentialTimeDecayedAvg",
        {createAggregateFunctionExponentialTimeDecayedAvg, {}, {.is_state = true}});

    factory.registerFunction("exponentialTimeDecayedCount",
        {createAggregateFunctionExponentialTimeDecayedCount, {}, {.is_state = true}});
}

}
