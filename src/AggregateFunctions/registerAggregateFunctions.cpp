#include <AggregateFunctions/registerAggregateFunctions.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>


namespace DB
{
struct Settings;

class AggregateFunctionFactory;
void registerAggregateFunctionAvg(AggregateFunctionFactory &);
void registerAggregateFunctionAvgWeighted(AggregateFunctionFactory &);
void registerAggregateFunctionCount(AggregateFunctionFactory &);
void registerAggregateFunctionDeltaSum(AggregateFunctionFactory &);
void registerAggregateFunctionDeltaSumTimestamp(AggregateFunctionFactory &);
void registerAggregateFunctionGroupArray(AggregateFunctionFactory &);
void registerAggregateFunctionGroupUniqArray(AggregateFunctionFactory &);
void registerAggregateFunctionGroupArrayInsertAt(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantile(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileDeterministic(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileExact(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileExactWeighted(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileInterpolatedWeighted(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileExactLow(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileExactHigh(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileExactInclusive(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileExactExclusive(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileTiming(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileTimingWeighted(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileTDigest(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileTDigestWeighted(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileBFloat16(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileBFloat16Weighted(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileApprox(AggregateFunctionFactory &);
void registerAggregateFunctionsSequenceMatch(AggregateFunctionFactory &);
void registerAggregateFunctionWindowFunnel(AggregateFunctionFactory &);
void registerAggregateFunctionRate(AggregateFunctionFactory &);
void registerAggregateFunctionsMin(AggregateFunctionFactory &);
void registerAggregateFunctionsMax(AggregateFunctionFactory &);
void registerAggregateFunctionsAny(AggregateFunctionFactory &);
void registerAggregateFunctionsStatisticsStable(AggregateFunctionFactory &);
void registerAggregateFunctionsStatisticsSecondMoment(AggregateFunctionFactory &);
void registerAggregateFunctionsStatisticsThirdMoment(AggregateFunctionFactory &);
void registerAggregateFunctionsStatisticsFourthMoment(AggregateFunctionFactory &);
void registerAggregateFunctionsStatisticsCovar(AggregateFunctionFactory &);
void registerAggregateFunctionsStatisticsCorr(AggregateFunctionFactory &);
void registerAggregateFunctionsVarianceMatrix(AggregateFunctionFactory &);
void registerAggregateFunctionSum(AggregateFunctionFactory &);
void registerAggregateFunctionSumCount(AggregateFunctionFactory &);
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
void registerAggregateFunctionMLMethod(AggregateFunctionFactory &);
void registerAggregateFunctionEntropy(AggregateFunctionFactory &);
void registerAggregateFunctionSimpleLinearRegression(AggregateFunctionFactory &);
void registerAggregateFunctionMoving(AggregateFunctionFactory &);
void registerAggregateFunctionCategoricalIV(AggregateFunctionFactory &);
void registerAggregateFunctionAggThrow(AggregateFunctionFactory &);
void registerAggregateFunctionRankCorrelation(AggregateFunctionFactory &);
void registerAggregateFunctionMannWhitney(AggregateFunctionFactory &);
void registerAggregateFunctionWelchTTest(AggregateFunctionFactory &);
void registerAggregateFunctionStudentTTest(AggregateFunctionFactory &);
void registerAggregateFunctionMeanZTest(AggregateFunctionFactory &);
void registerAggregateFunctionCramersV(AggregateFunctionFactory &);
void registerAggregateFunctionTheilsU(AggregateFunctionFactory &);
void registerAggregateFunctionContingency(AggregateFunctionFactory &);
void registerAggregateFunctionCramersVBiasCorrected(AggregateFunctionFactory &);
void registerAggregateFunctionSingleValueOrNull(AggregateFunctionFactory &);
void registerAggregateFunctionSequenceNextNode(AggregateFunctionFactory &);
void registerAggregateFunctionNothing(AggregateFunctionFactory &);
void registerAggregateFunctionExponentialMovingAverage(AggregateFunctionFactory &);
void registerAggregateFunctionSparkbar(AggregateFunctionFactory &);
void registerAggregateFunctionIntervalLengthSum(AggregateFunctionFactory &);
void registerAggregateFunctionAnalysisOfVariance(AggregateFunctionFactory &);
void registerAggregateFunctionFlameGraph(AggregateFunctionFactory &);
void registerAggregateFunctionKolmogorovSmirnovTest(AggregateFunctionFactory & factory);

class AggregateFunctionCombinatorFactory;
void registerAggregateFunctionCombinatorIf(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorArray(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorForEach(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorSimpleState(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorState(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorMerge(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorNull(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorOrFill(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorResample(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorDistinct(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorMap(AggregateFunctionCombinatorFactory & factory);

void registerWindowFunctions(AggregateFunctionFactory & factory);

void registerAggregateFunctions()
{
    {
        auto & factory = AggregateFunctionFactory::instance();

        registerAggregateFunctionAvg(factory);
        registerAggregateFunctionAvgWeighted(factory);
        registerAggregateFunctionCount(factory);
        registerAggregateFunctionDeltaSum(factory);
        registerAggregateFunctionDeltaSumTimestamp(factory);
        registerAggregateFunctionGroupArray(factory);
        registerAggregateFunctionGroupUniqArray(factory);
        registerAggregateFunctionGroupArrayInsertAt(factory);
        registerAggregateFunctionsQuantile(factory);
        registerAggregateFunctionsQuantileDeterministic(factory);
        registerAggregateFunctionsQuantileExact(factory);
        registerAggregateFunctionsQuantileExactWeighted(factory);
        registerAggregateFunctionsQuantileInterpolatedWeighted(factory);
        registerAggregateFunctionsQuantileExactLow(factory);
        registerAggregateFunctionsQuantileExactHigh(factory);
        registerAggregateFunctionsQuantileExactInclusive(factory);
        registerAggregateFunctionsQuantileExactExclusive(factory);
        registerAggregateFunctionsQuantileTiming(factory);
        registerAggregateFunctionsQuantileTimingWeighted(factory);
        registerAggregateFunctionsQuantileTDigest(factory);
        registerAggregateFunctionsQuantileTDigestWeighted(factory);
        registerAggregateFunctionsQuantileBFloat16(factory);
        registerAggregateFunctionsQuantileBFloat16Weighted(factory);
        registerAggregateFunctionsQuantileApprox(factory);
        registerAggregateFunctionsSequenceMatch(factory);
        registerAggregateFunctionWindowFunnel(factory);
        registerAggregateFunctionRate(factory);
        registerAggregateFunctionsMin(factory);
        registerAggregateFunctionsMax(factory);
        registerAggregateFunctionsAny(factory);
        registerAggregateFunctionsStatisticsStable(factory);
        registerAggregateFunctionsStatisticsSecondMoment(factory);
        registerAggregateFunctionsStatisticsThirdMoment(factory);
        registerAggregateFunctionsStatisticsFourthMoment(factory);
        registerAggregateFunctionsStatisticsCovar(factory);
        registerAggregateFunctionsStatisticsCorr(factory);
        registerAggregateFunctionsVarianceMatrix(factory);
        registerAggregateFunctionSum(factory);
        registerAggregateFunctionSumCount(factory);
        registerAggregateFunctionSumMap(factory);
        registerAggregateFunctionsUniq(factory);
        registerAggregateFunctionUniqCombined(factory);
        registerAggregateFunctionUniqUpTo(factory);
        registerAggregateFunctionTopK(factory);
        registerAggregateFunctionsBitwise(factory);
        registerAggregateFunctionCramersV(factory);
        registerAggregateFunctionTheilsU(factory);
        registerAggregateFunctionContingency(factory);
        registerAggregateFunctionCramersVBiasCorrected(factory);
        registerAggregateFunctionsBitmap(factory);
        registerAggregateFunctionsMaxIntersections(factory);
        registerAggregateFunctionHistogram(factory);
        registerAggregateFunctionRetention(factory);
        registerAggregateFunctionMLMethod(factory);
        registerAggregateFunctionEntropy(factory);
        registerAggregateFunctionSimpleLinearRegression(factory);
        registerAggregateFunctionMoving(factory);
        registerAggregateFunctionCategoricalIV(factory);
        registerAggregateFunctionAggThrow(factory);
        registerAggregateFunctionRankCorrelation(factory);
        registerAggregateFunctionMannWhitney(factory);
        registerAggregateFunctionSequenceNextNode(factory);
        registerAggregateFunctionWelchTTest(factory);
        registerAggregateFunctionStudentTTest(factory);
        registerAggregateFunctionMeanZTest(factory);
        registerAggregateFunctionNothing(factory);
        registerAggregateFunctionSingleValueOrNull(factory);
        registerAggregateFunctionIntervalLengthSum(factory);
        registerAggregateFunctionExponentialMovingAverage(factory);
        registerAggregateFunctionSparkbar(factory);
        registerAggregateFunctionAnalysisOfVariance(factory);
        registerAggregateFunctionFlameGraph(factory);
        registerAggregateFunctionKolmogorovSmirnovTest(factory);

        registerWindowFunctions(factory);
    }

    {
        auto & factory = AggregateFunctionCombinatorFactory::instance();

        registerAggregateFunctionCombinatorIf(factory);
        registerAggregateFunctionCombinatorArray(factory);
        registerAggregateFunctionCombinatorForEach(factory);
        registerAggregateFunctionCombinatorSimpleState(factory);
        registerAggregateFunctionCombinatorState(factory);
        registerAggregateFunctionCombinatorMerge(factory);
        registerAggregateFunctionCombinatorNull(factory);
        registerAggregateFunctionCombinatorOrFill(factory);
        registerAggregateFunctionCombinatorResample(factory);
        registerAggregateFunctionCombinatorDistinct(factory);
        registerAggregateFunctionCombinatorMap(factory);
    }
}

}
