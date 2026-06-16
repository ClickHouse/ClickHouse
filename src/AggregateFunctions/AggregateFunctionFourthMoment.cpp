#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T> using AggregateFunctionFourthMoment = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 4>>;

void registerAggregateFunctionsStatisticsFourthMoment(AggregateFunctionFactory & factory)
{
    factory.registerFunction("kurtSamp", createAggregateFunctionStatisticsUnary<AggregateFunctionFourthMoment, StatisticsFunctionKind::kurtSamp>);
    factory.registerFunction("kurtPop", createAggregateFunctionStatisticsUnary<AggregateFunctionFourthMoment, StatisticsFunctionKind::kurtPop>);
}

}
