#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionStatistics.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <template <typename> typename FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsUnary(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res(createWithNumericType<FunctionTemplate>(*argument_types[0], argument_types[0]));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument for aggregate function {}", argument_types[0]->getName(), name);

    return res;
}

template <template <typename, typename> typename FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsBinary(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res(createWithTwoBasicNumericTypes<FunctionTemplate>(*argument_types[0], *argument_types[1], argument_types));
    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal types {} and {} of arguments for aggregate function {}",
            argument_types[0]->getName(), argument_types[1]->getName(), name);

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
