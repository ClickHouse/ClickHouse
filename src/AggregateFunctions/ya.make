# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
)


SRCS(
    AggregateFunctionAggThrow.cpp
    AggregateFunctionAny.cpp
    AggregateFunctionArray.cpp
    AggregateFunctionAvg.cpp
    AggregateFunctionAvgWeighted.cpp
    AggregateFunctionBitwise.cpp
    AggregateFunctionBoundingRatio.cpp
    AggregateFunctionCategoricalInformationValue.cpp
    AggregateFunctionCombinatorFactory.cpp
    AggregateFunctionCount.cpp
    AggregateFunctionDeltaSum.cpp
    AggregateFunctionDeltaSumTimestamp.cpp
    AggregateFunctionDistinct.cpp
    AggregateFunctionEntropy.cpp
    AggregateFunctionFactory.cpp
    AggregateFunctionForEach.cpp
    AggregateFunctionGroupArray.cpp
    AggregateFunctionGroupArrayInsertAt.cpp
    AggregateFunctionGroupArrayMoving.cpp
    AggregateFunctionGroupUniqArray.cpp
    AggregateFunctionHistogram.cpp
    AggregateFunctionIf.cpp
    AggregateFunctionIntervalLengthSum.cpp
    AggregateFunctionMLMethod.cpp
    AggregateFunctionMannWhitney.cpp
    AggregateFunctionMax.cpp
    AggregateFunctionMaxIntersections.cpp
    AggregateFunctionMerge.cpp
    AggregateFunctionMin.cpp
    AggregateFunctionNull.cpp
    AggregateFunctionOrFill.cpp
    AggregateFunctionQuantile.cpp
    AggregateFunctionRankCorrelation.cpp
    AggregateFunctionResample.cpp
    AggregateFunctionRetention.cpp
    AggregateFunctionSequenceMatch.cpp
    AggregateFunctionSequenceNextNode.cpp
    AggregateFunctionSimpleLinearRegression.cpp
    AggregateFunctionSimpleState.cpp
    AggregateFunctionState.cpp
    AggregateFunctionStatistics.cpp
    AggregateFunctionStatisticsSimple.cpp
    AggregateFunctionStudentTTest.cpp
    AggregateFunctionSum.cpp
    AggregateFunctionSumCount.cpp
    AggregateFunctionSumMap.cpp
    AggregateFunctionTopK.cpp
    AggregateFunctionUniq.cpp
    AggregateFunctionUniqCombined.cpp
    AggregateFunctionUniqUpTo.cpp
    AggregateFunctionWelchTTest.cpp
    AggregateFunctionWindowFunnel.cpp
    IAggregateFunction.cpp
    UniqCombinedBiasData.cpp
    UniqVariadicHash.cpp
    parseAggregateFunctionParameters.cpp
    registerAggregateFunctions.cpp

)

END()
