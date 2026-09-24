#include <WindowFunctions/registerWindowFunctions.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

void registerWindowFunctionsRanking(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsDistribution(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsLagLead(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsNthValue(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsExponentialTimeDecayed(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsNonNegativeDerivative(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);

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

    registerWindowFunctionsRanking(factory, properties);
    registerWindowFunctionsDistribution(factory, properties);
    registerWindowFunctionsNthValue(factory, properties);
    registerWindowFunctionsLagLead(factory, properties);
    registerWindowFunctionsExponentialTimeDecayed(factory, properties);
    registerWindowFunctionsNonNegativeDerivative(factory, properties);
}

}
