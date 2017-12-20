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
void registerAggregateFunctionsSequenceMatch(AggregateFunctionFactory & factory);
void registerAggregateFunctionsMinMaxAny(AggregateFunctionFactory & factory);
void registerAggregateFunctionsStatistics(AggregateFunctionFactory & factory);
void registerAggregateFunctionSum(AggregateFunctionFactory & factory);
void registerAggregateFunctionSumMap(AggregateFunctionFactory & factory);
void registerAggregateFunctionsUniq(AggregateFunctionFactory & factory);
void registerAggregateFunctionUniqUpTo(AggregateFunctionFactory & factory);
void registerAggregateFunctionTopK(AggregateFunctionFactory & factory);
void registerAggregateFunctionsBitwise(AggregateFunctionFactory & factory);


void registerAggregateFunctions()
{
    auto & factory = AggregateFunctionFactory::instance();

    registerAggregateFunctionAvg(factory);
    registerAggregateFunctionCount(factory);
    registerAggregateFunctionGroupArray(factory);
    registerAggregateFunctionGroupUniqArray(factory);
    registerAggregateFunctionGroupArrayInsertAt(factory);
    registerAggregateFunctionsQuantile(factory);
    registerAggregateFunctionsSequenceMatch(factory);
    registerAggregateFunctionsMinMaxAny(factory);
    registerAggregateFunctionsStatistics(factory);
    registerAggregateFunctionSum(factory);
    registerAggregateFunctionSumMap(factory);
    registerAggregateFunctionsUniq(factory);
    registerAggregateFunctionUniqUpTo(factory);
    registerAggregateFunctionTopK(factory);
    registerAggregateFunctionsBitwise(factory);
}

}
