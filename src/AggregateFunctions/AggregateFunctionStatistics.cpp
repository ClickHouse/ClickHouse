#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionStatistics.h>
#include "registerAggregateFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <template <typename> class FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsUnary(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res(createWithNumericType<FunctionTemplate>(*argument_types[0], argument_types[0]));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

template <template <typename, typename> class FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsBinary(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res(createWithTwoNumericTypes<FunctionTemplate>(*argument_types[0], *argument_types[1], argument_types));
    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

}

void registerAggregateFunctionsStatisticsStable(AggregateFunctionFactory & factory)
{
    factory.registerFunction("varSampStable", createAggregateFunctionStatisticsUnary<AggregateFunctionVarSampStable>);
    factory.registerFunction("varPopStable", createAggregateFunctionStatisticsUnary<AggregateFunctionVarPopStable>);
    factory.registerFunction("stddevSampStable", createAggregateFunctionStatisticsUnary<AggregateFunctionStddevSampStable>);
    factory.registerFunction("stddevPopStable", createAggregateFunctionStatisticsUnary<AggregateFunctionStddevPopStable>);
    factory.registerFunction("covarSampStable", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarSampStable>);
    factory.registerFunction("covarPopStable", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarPopStable>);
    factory.registerFunction("corrStable", createAggregateFunctionStatisticsBinary<AggregateFunctionCorrStable>);
}

}
