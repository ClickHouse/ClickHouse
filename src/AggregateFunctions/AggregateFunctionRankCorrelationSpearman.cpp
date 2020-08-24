#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionRankCorrelationSpearman.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include "registerAggregateFunctions.h"
#include <AggregateFunctions/Helpers.h>


namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionRankCorrelationSpearman(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertBinary(name, argument_types);
    assertNoParameters(name, parameters);

    if (isDecimal(argument_types[0]) || isDecimal(argument_types[1]))
    {
        throw Exception("Aggregate function " + name + " only supports numerical types.", ErrorCodes::NOT_IMPLEMENTED);
    }

    AggregateFunctionPtr res(createWithTwoNumericTypes<AggregateFunctionRankCorrelationSpearman>(*argument_types[0], *argument_types[1], argument_types));
    return res;
}

}


void registerAggregateFunctionRankCorrelationSpearman (AggregateFunctionFactory & factory)
{
    factory.registerFunction("rankCorrelationSpearman", createAggregateFunctionRankCorrelationSpearman, AggregateFunctionFactory::CaseInsensitive);
}

}
