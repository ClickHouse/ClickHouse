#include <AggregateFunctions/registerAggregateFunctions.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>


namespace DB
{

void registerAggregateFunctionAvg(AggregateFunctionFactory &);
void registerAggregateFunctionCount(AggregateFunctionFactory &);
void registerAggregateFunctionGroupArray(AggregateFunctionFactory &);
void registerAggregateFunctionGroupUniqArray(AggregateFunctionFactory &);
void registerAggregateFunctionGroupArrayInsertAt(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantile(AggregateFunctionFactory &);
void registerAggregateFunctionsSequenceMatch(AggregateFunctionFactory &);
void registerAggregateFunctionWindowFunnel(AggregateFunctionFactory &);
void registerAggregateFunctionsMinMaxAny(AggregateFunctionFactory &);
void registerAggregateFunctionsStatisticsStable(AggregateFunctionFactory &);
void registerAggregateFunctionsStatisticsSimple(AggregateFunctionFactory &);
void registerAggregateFunctionSum(AggregateFunctionFactory &);
void registerAggregateFunctionSumMap(AggregateFunctionFactory &);
void registerAggregateFunctionsUniq(AggregateFunctionFactory &);
void registerAggregateFunctionUniqUpTo(AggregateFunctionFactory &);
void registerAggregateFunctionTopK(AggregateFunctionFactory &);
void registerAggregateFunctionsBitwise(AggregateFunctionFactory &);
void registerAggregateFunctionsMaxIntersections(AggregateFunctionFactory &);

void registerAggregateFunctionCombinatorIf(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorArray(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorForEach(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorState(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorMerge(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorNull(AggregateFunctionCombinatorFactory &);


void registerAggregateFunctions()
{
    {
        auto & factory = AggregateFunctionFactory::instance();

        registerAggregateFunctionAvg(factory);
        registerAggregateFunctionCount(factory);
        registerAggregateFunctionGroupArray(factory);
        registerAggregateFunctionGroupUniqArray(factory);
        registerAggregateFunctionGroupArrayInsertAt(factory);
        registerAggregateFunctionsQuantile(factory);
        registerAggregateFunctionsSequenceMatch(factory);
        registerAggregateFunctionWindowFunnel(factory);
        registerAggregateFunctionsMinMaxAny(factory);
        registerAggregateFunctionsStatisticsStable(factory);
        registerAggregateFunctionsStatisticsSimple(factory);
        registerAggregateFunctionSum(factory);
        registerAggregateFunctionSumMap(factory);
        registerAggregateFunctionsUniq(factory);
        registerAggregateFunctionUniqUpTo(factory);
        registerAggregateFunctionTopK(factory);
        registerAggregateFunctionsBitwise(factory);
        registerAggregateFunctionsMaxIntersections(factory);
    }

    {
        auto & factory = AggregateFunctionCombinatorFactory::instance();

        registerAggregateFunctionCombinatorIf(factory);
        registerAggregateFunctionCombinatorArray(factory);
        registerAggregateFunctionCombinatorForEach(factory);
        registerAggregateFunctionCombinatorState(factory);
        registerAggregateFunctionCombinatorMerge(factory);
        registerAggregateFunctionCombinatorNull(factory);
    }
}

}
