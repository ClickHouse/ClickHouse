#include <AggregateFunctions/AggregateFunctionStatisticsSimpleMatrix.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsArbitrary(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    for (const auto & argument_type : argument_types)
        if (!isNativeNumber(argument_type))
            throw Exception("Aggregate function " + name + " only supports numerical types", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<FunctionTemplate>(argument_types);
}

}

void registerAggregateFunctionsStatisticsSimpleMatrix(AggregateFunctionFactory & factory) {
    factory.registerFunction("covarSampMatrix", createAggregateFunctionStatisticsArbitrary<AggregateFunctionCovarSampSimpleMatrix>);
    factory.registerFunction("covarPopMatrix", createAggregateFunctionStatisticsArbitrary<AggregateFunctionCovarPopSimpleMatrix>);
    factory.registerFunction("corrMatrix", createAggregateFunctionStatisticsArbitrary<AggregateFunctionCorrSimpleMatrix>);
}

}
