#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionRankCorrelation.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace DB
{
struct Settings;

namespace
{

AggregateFunctionPtr createAggregateFunctionRankCorrelation(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);
    assertNoParameters(name, parameters);

    if (!isNumber(argument_types[0]) || !isNumber(argument_types[1]))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregate function {} only supports numerical types", name);

    return std::make_shared<AggregateFunctionRankCorrelation>(argument_types);
}

}


void registerAggregateFunctionRankCorrelation(AggregateFunctionFactory & factory)
{
    factory.registerFunction("rankCorr", createAggregateFunctionRankCorrelation);
}

}
