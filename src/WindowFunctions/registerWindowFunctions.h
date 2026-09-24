#pragma once

namespace DB
{

class AggregateFunctionFactory;
struct AggregateFunctionProperties;

void registerWindowFunctionsRanking(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsDistribution(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsLagLead(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsNthValue(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsExponentialTimeDecayed(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsNonNegativeDerivative(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);

}
