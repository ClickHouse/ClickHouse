#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionAnalysisOfVariance.h>
#include <AggregateFunctions/FactoryHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionAnalysisOfVariance(const std::string & name, const DataTypes & arguments, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, arguments);

    if (!isNumber(arguments[0]) || !isNumber(arguments[1]))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} only supports numerical types", name);

    return std::make_shared<AggregateFunctionAnalysisOfVariance>(arguments, parameters);
}

}

void registerAggregateFunctionAnalysisOfVariance(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .is_order_dependent = false };
    factory.registerFunction("analysisOfVariance", {createAggregateFunctionAnalysisOfVariance, properties}, AggregateFunctionFactory::CaseInsensitive);

    /// This is widely used term
    factory.registerAlias("anova", "analysisOfVariance", AggregateFunctionFactory::CaseInsensitive);
}

}
