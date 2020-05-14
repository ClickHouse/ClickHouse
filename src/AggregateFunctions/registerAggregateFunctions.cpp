#include <AggregateFunctions/registerAggregateFunctions.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>


namespace DB
{

void registerAggregateFunctions()
{
    {
        auto & factory = AggregateFunctionFactory::instance();

        registerAggregateFunctionAvg(factory);
        registerAggregateFunctionAvgWeighted(factory);
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
#if !defined(ARCADIA_BUILD)
        registerAggregateFunctionsBitmap(factory);
#endif
        registerAggregateFunctionsMaxIntersections(factory);
        registerAggregateFunctionHistogram(factory);
        registerAggregateFunctionRetention(factory);
        registerAggregateFunctionTimeSeriesGroupSum(factory);
        registerAggregateFunctionMLMethod(factory);
        registerAggregateFunctionEntropy(factory);
        registerAggregateFunctionSimpleLinearRegression(factory);
        registerAggregateFunctionMoving(factory);
        registerAggregateFunctionCategoricalIV(factory);
        registerAggregateFunctionAggThrow(factory);
    }

    {
        auto & factory = AggregateFunctionCombinatorFactory::instance();

        registerAggregateFunctionCombinatorIf(factory);
        registerAggregateFunctionCombinatorArray(factory);
        registerAggregateFunctionCombinatorForEach(factory);
        registerAggregateFunctionCombinatorState(factory);
        registerAggregateFunctionCombinatorMerge(factory);
        registerAggregateFunctionCombinatorNull(factory);
        registerAggregateFunctionCombinatorOrFill(factory);
        registerAggregateFunctionCombinatorResample(factory);
        registerAggregateFunctionCombinatorDistinct(factory);
    }
}

}
