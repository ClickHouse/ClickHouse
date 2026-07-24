#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T1, typename T2> using AggregateFunctionCorr = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, CorrMoments>>;

void registerAggregateFunctionsStatisticsCorr(AggregateFunctionFactory & factory)
{
    factory.registerFunction("corr", createAggregateFunctionStatisticsBinary<AggregateFunctionCorr, StatisticsFunctionKind::corr>, AggregateFunctionFactory::Case::Insensitive);
}

}
