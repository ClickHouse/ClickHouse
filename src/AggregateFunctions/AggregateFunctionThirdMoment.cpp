#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T> using AggregateFunctionThirdMoment = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 3>>;

void registerAggregateFunctionsStatisticsThirdMoment(AggregateFunctionFactory & factory)
{
    factory.registerFunction("skewSamp", createAggregateFunctionStatisticsUnary<AggregateFunctionThirdMoment, StatisticsFunctionKind::skewSamp>);
    factory.registerFunction("skewPop", createAggregateFunctionStatisticsUnary<AggregateFunctionThirdMoment, StatisticsFunctionKind::skewPop>);
}

}
