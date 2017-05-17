#include <AggregateFunctions/registerAggregateFunctions.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

void registerAggregateFunctionAvg(AggregateFunctionFactory & factory);
void registerAggregateFunctionCount(AggregateFunctionFactory & factory);
void registerAggregateFunctionGroupArray(AggregateFunctionFactory & factory);
void registerAggregateFunctionGroupUniqArray(AggregateFunctionFactory & factory);
void registerAggregateFunctionGroupArrayInsertAt(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantile(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileExact(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileExactWeighted(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileDeterministic(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileTiming(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileTDigest(AggregateFunctionFactory & factory);
void registerAggregateFunctionsSequenceMatch(AggregateFunctionFactory & factory);
void registerAggregateFunctionsMinMaxAny(AggregateFunctionFactory & factory);
void registerAggregateFunctionsStatistics(AggregateFunctionFactory & factory);
void registerAggregateFunctionSum(AggregateFunctionFactory & factory);
void registerAggregateFunctionsUniq(AggregateFunctionFactory & factory);
void registerAggregateFunctionUniqUpTo(AggregateFunctionFactory & factory);
void registerAggregateFunctionTopK(AggregateFunctionFactory & factory);
void registerAggregateFunctionDebug(AggregateFunctionFactory & factory);


void registerAggregateFunctions()
{
    auto & factory = AggregateFunctionFactory::instance();

    registerAggregateFunctionAvg(factory);
    registerAggregateFunctionCount(factory);
    registerAggregateFunctionGroupArray(factory);
    registerAggregateFunctionGroupUniqArray(factory);
    registerAggregateFunctionGroupArrayInsertAt(factory);
    registerAggregateFunctionsQuantile(factory);
    registerAggregateFunctionsQuantileExact(factory);
    registerAggregateFunctionsQuantileExactWeighted(factory);
    registerAggregateFunctionsQuantileDeterministic(factory);
    registerAggregateFunctionsQuantileTiming(factory);
    registerAggregateFunctionsQuantileTDigest(factory);
    registerAggregateFunctionsSequenceMatch(factory);
    registerAggregateFunctionsMinMaxAny(factory);
    registerAggregateFunctionsStatistics(factory);
    registerAggregateFunctionSum(factory);
    registerAggregateFunctionsUniq(factory);
    registerAggregateFunctionUniqUpTo(factory);
    registerAggregateFunctionTopK(factory);
    registerAggregateFunctionDebug(factory);
}

}
