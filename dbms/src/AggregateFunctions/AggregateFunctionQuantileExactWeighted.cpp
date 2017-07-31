#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/AggregateFunctionQuantileExactWeighted.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionQuantileExactWeighted(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    AggregateFunctionPtr res(createWithTwoNumericTypes<AggregateFunctionQuantileExactWeighted>(*argument_types[0], *argument_types[1]));

    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

AggregateFunctionPtr createAggregateFunctionQuantilesExactWeighted(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    AggregateFunctionPtr res(createWithTwoNumericTypes<AggregateFunctionQuantilesExactWeighted>(*argument_types[0], *argument_types[1]));

    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}


}

void registerAggregateFunctionsQuantileExactWeighted(AggregateFunctionFactory & factory)
{
    factory.registerFunction("quantileExactWeighted", createAggregateFunctionQuantileExactWeighted);
    factory.registerFunction("medianExactWeighted", createAggregateFunctionQuantileExactWeighted);
    factory.registerFunction("quantilesExactWeighted", createAggregateFunctionQuantilesExactWeighted);
}

}
