#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/AggregateFunctionQuantileTiming.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionQuantileTiming(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionQuantileTiming>(*argument_types[0]));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

AggregateFunctionPtr createAggregateFunctionQuantilesTiming(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionQuantilesTiming>(*argument_types[0]));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

AggregateFunctionPtr createAggregateFunctionQuantileTimingWeighted(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    AggregateFunctionPtr res(createWithTwoNumericTypes<AggregateFunctionQuantileTimingWeighted>(*argument_types[0], *argument_types[1]));

    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

AggregateFunctionPtr createAggregateFunctionQuantilesTimingWeighted(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    AggregateFunctionPtr res(createWithTwoNumericTypes<AggregateFunctionQuantilesTimingWeighted>(*argument_types[0], *argument_types[1]));

    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}


}

void registerAggregateFunctionsQuantileTiming(AggregateFunctionFactory & factory)
{
    factory.registerFunction("quantileTiming", createAggregateFunctionQuantileTiming);
    factory.registerFunction("medianTiming", createAggregateFunctionQuantileTiming);
    factory.registerFunction("quantilesTiming", createAggregateFunctionQuantilesTiming);
    factory.registerFunction("quantileTimingWeighted", createAggregateFunctionQuantileTimingWeighted);
    factory.registerFunction("medianTimingWeighted", createAggregateFunctionQuantileTimingWeighted);
    factory.registerFunction("quantilesTimingWeighted", createAggregateFunctionQuantilesTimingWeighted);
}

}
