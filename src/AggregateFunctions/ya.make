# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

SRCS(
    AggregateFunctionAggThrow.cpp
    AggregateFunctionArray.cpp
    AggregateFunctionAvg.cpp
    AggregateFunctionAvgWeighted.cpp
    AggregateFunctionBitwise.cpp
    AggregateFunctionBoundingRatio.cpp
    AggregateFunctionCategoricalInformationValue.cpp
    AggregateFunctionCombinatorFactory.cpp
    AggregateFunctionCount.cpp
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
    AggregateFunctionMaxIntersections.cpp
    AggregateFunctionMerge.cpp
    AggregateFunctionMinMaxAny.cpp
    AggregateFunctionMLMethod.cpp
    AggregateFunctionNull.cpp
    AggregateFunctionOrFill.cpp
    AggregateFunctionQuantile.cpp
    AggregateFunctionResample.cpp
    AggregateFunctionRetention.cpp
    AggregateFunctionSequenceMatch.cpp
    AggregateFunctionSimpleLinearRegression.cpp
    AggregateFunctionState.cpp
    AggregateFunctionStatistics.cpp
    AggregateFunctionStatisticsSimple.cpp
    AggregateFunctionSum.cpp
    AggregateFunctionSumMap.cpp
    AggregateFunctionTimeSeriesGroupSum.cpp
    AggregateFunctionTopK.cpp
    AggregateFunctionUniqCombined.cpp
    AggregateFunctionUniq.cpp
    AggregateFunctionUniqUpTo.cpp
    AggregateFunctionWindowFunnel.cpp
    parseAggregateFunctionParameters.cpp
    registerAggregateFunctions.cpp
    UniqCombinedBiasData.cpp
    UniqVariadicHash.cpp

)

END()
