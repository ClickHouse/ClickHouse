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
void registerAggregateFunctionRate(AggregateFunctionFactory &);
void registerAggregateFunctionsMinMaxAny(AggregateFunctionFactory &);
void registerAggregateFunctionsStatisticsStable(AggregateFunctionFactory &);
void registerAggregateFunctionsStatisticsSimple(AggregateFunctionFactory &);
void registerAggregateFunctionSum(AggregateFunctionFactory &);
void registerAggregateFunctionSumMap(AggregateFunctionFactory &);
void registerAggregateFunctionsUniq(AggregateFunctionFactory &);
void registerAggregateFunctionUniqCombined(AggregateFunctionFactory &);
void registerAggregateFunctionUniqUpTo(AggregateFunctionFactory &);
void registerAggregateFunctionTopK(AggregateFunctionFactory &);
void registerAggregateFunctionsBitwise(AggregateFunctionFactory &);
void registerAggregateFunctionsBitmap(AggregateFunctionFactory &);
void registerAggregateFunctionsMaxIntersections(AggregateFunctionFactory &);
void registerAggregateFunctionHistogram(AggregateFunctionFactory &);
void registerAggregateFunctionRetention(AggregateFunctionFactory &);
void registerAggregateFunctionTimeSeriesGroupSum(AggregateFunctionFactory &);
void registerAggregateFunctionMLMethod(AggregateFunctionFactory &);
void registerAggregateFunctionEntropy(AggregateFunctionFactory &);
void registerAggregateFunctionSimpleLinearRegression(AggregateFunctionFactory &);
void registerAggregateFunctionMoving(AggregateFunctionFactory &);

void registerAggregateFunctionCombinatorIf(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorArray(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorForEach(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorState(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorMerge(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorNull(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorResample(AggregateFunctionCombinatorFactory &);

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
        registerAggregateFunctionRate(factory);
        registerAggregateFunctionsMinMaxAny(factory);
        registerAggregateFunctionsStatisticsStable(factory);
        registerAggregateFunctionsStatisticsSimple(factory);
        registerAggregateFunctionSum(factory);
        registerAggregateFunctionSumMap(factory);
        registerAggregateFunctionsUniq(factory);
        registerAggregateFunctionUniqCombined(factory);
        registerAggregateFunctionUniqUpTo(factory);
        registerAggregateFunctionTopK(factory);
        registerAggregateFunctionsBitwise(factory);
        registerAggregateFunctionsBitmap(factory);
        registerAggregateFunctionsMaxIntersections(factory);
        registerAggregateFunctionHistogram(factory);
        registerAggregateFunctionRetention(factory);
        registerAggregateFunctionTimeSeriesGroupSum(factory);
        registerAggregateFunctionMLMethod(factory);
        registerAggregateFunctionEntropy(factory);
        registerAggregateFunctionSimpleLinearRegression(factory);
        registerAggregateFunctionMoving(factory);
    }

    {
        auto & factory = AggregateFunctionCombinatorFactory::instance();

        registerAggregateFunctionCombinatorIf(factory);
        registerAggregateFunctionCombinatorArray(factory);
        registerAggregateFunctionCombinatorForEach(factory);
        registerAggregateFunctionCombinatorState(factory);
        registerAggregateFunctionCombinatorMerge(factory);
        registerAggregateFunctionCombinatorNull(factory);
        registerAggregateFunctionCombinatorResample(factory);
    }
}

}
