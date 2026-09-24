#include <WindowFunctions/registerWindowFunctions.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

void registerWindowFunctionRank(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionDenseRank(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionPercentRank(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionCumeDist(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionRowNumber(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionNtile(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionNthValue(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsLagLead(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsExponentialTimeDecayed(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionNonNegativeDerivative(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);

void registerWindowFunctions(AggregateFunctionFactory & factory)
{
    const AggregateFunctionProperties properties = {
        // By default, if an aggregate function has a null argument, it will be
        // replaced with AggregateFunctionNothing. We don't need this behavior
        // e.g. for lagInFrame(number, 1, null).
        .returns_default_when_only_null = true,
        // This probably doesn't make any difference for window functions because
        // it is an Aggregator-specific setting.
        .is_order_dependent = true,
        .is_window_function = true};

    registerWindowFunctionRank(factory, properties);
    registerWindowFunctionDenseRank(factory, properties);
    registerWindowFunctionPercentRank(factory, properties);
    registerWindowFunctionCumeDist(factory, properties);
    registerWindowFunctionRowNumber(factory, properties);
    registerWindowFunctionNtile(factory, properties);
    registerWindowFunctionNthValue(factory, properties);
    registerWindowFunctionsLagLead(factory, properties);
    registerWindowFunctionsExponentialTimeDecayed(factory, properties);
    registerWindowFunctionNonNegativeDerivative(factory, properties);
}

}
