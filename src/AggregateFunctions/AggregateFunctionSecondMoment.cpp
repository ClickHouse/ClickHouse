#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T> using AggregateFunctionSecondMoment = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 2>>;

void registerAggregateFunctionsStatisticsSecondMoment(AggregateFunctionFactory & factory)
{
    factory.registerFunction("varSamp", createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::varSamp>);
    factory.registerFunction("varPop", createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::varPop>);
    factory.registerFunction("stddevSamp", createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::stddevSamp>);
    factory.registerFunction("stddevPop", createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::stddevPop>);

    /// Synonyms for compatibility.
    factory.registerAlias("VAR_SAMP", "varSamp", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias("VAR_POP", "varPop", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias("STDDEV_SAMP", "stddevSamp", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias("STDDEV_POP", "stddevPop", AggregateFunctionFactory::CaseInsensitive);
}

}
