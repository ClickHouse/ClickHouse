#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T> using AggregateFunctionVarPopSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 2>>;
template <typename T> using AggregateFunctionVarSampSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 2>>;
template <typename T> using AggregateFunctionStddevPopSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 2>>;
template <typename T> using AggregateFunctionStddevSampSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 2>>;
template <typename T> using AggregateFunctionSkewPopSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 3>>;
template <typename T> using AggregateFunctionSkewSampSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 3>>;
template <typename T> using AggregateFunctionKurtPopSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 4>>;
template <typename T> using AggregateFunctionKurtSampSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 4>>;
template <typename T1, typename T2> using AggregateFunctionCovarPopSimple = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, CovarMoments>>;
template <typename T1, typename T2> using AggregateFunctionCovarSampSimple = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, CovarMoments>>;
template <typename T1, typename T2> using AggregateFunctionCorrSimple = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, CorrMoments>>;

void registerAggregateFunctionsStatisticsSimple(AggregateFunctionFactory & factory)
{
    factory.registerFunction("varSamp", createAggregateFunctionStatisticsUnary<AggregateFunctionVarSampSimple, StatisticsFunctionKind::varSamp>);
    factory.registerFunction("varPop", createAggregateFunctionStatisticsUnary<AggregateFunctionVarPopSimple, StatisticsFunctionKind::varPop>);
    factory.registerFunction("stddevSamp", createAggregateFunctionStatisticsUnary<AggregateFunctionStddevSampSimple, StatisticsFunctionKind::stddevSamp>);
    factory.registerFunction("stddevPop", createAggregateFunctionStatisticsUnary<AggregateFunctionStddevPopSimple, StatisticsFunctionKind::stddevPop>);
    factory.registerFunction("skewSamp", createAggregateFunctionStatisticsUnary<AggregateFunctionSkewSampSimple, StatisticsFunctionKind::skewSamp>);
    factory.registerFunction("skewPop", createAggregateFunctionStatisticsUnary<AggregateFunctionSkewPopSimple, StatisticsFunctionKind::skewPop>);
    factory.registerFunction("kurtSamp", createAggregateFunctionStatisticsUnary<AggregateFunctionKurtSampSimple, StatisticsFunctionKind::kurtSamp>);
    factory.registerFunction("kurtPop", createAggregateFunctionStatisticsUnary<AggregateFunctionKurtPopSimple, StatisticsFunctionKind::kurtPop>);

    factory.registerFunction("covarSamp", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarSampSimple, StatisticsFunctionKind::covarSamp>);
    factory.registerFunction("covarPop", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarPopSimple, StatisticsFunctionKind::covarPop>);
    factory.registerFunction("corr", createAggregateFunctionStatisticsBinary<AggregateFunctionCorrSimple, StatisticsFunctionKind::corr>, AggregateFunctionFactory::CaseInsensitive);

    /// Synonims for compatibility.
    factory.registerAlias("VAR_SAMP", "varSamp", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias("VAR_POP", "varPop", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias("STDDEV_SAMP", "stddevSamp", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias("STDDEV_POP", "stddevPop", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias("COVAR_SAMP", "covarSamp", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias("COVAR_POP", "covarPop", AggregateFunctionFactory::CaseInsensitive);
}

}
