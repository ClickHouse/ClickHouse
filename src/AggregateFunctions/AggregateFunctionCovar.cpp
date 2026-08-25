#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T1, typename T2> using AggregateFunctionCovar = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, CovarMoments>>;

void registerAggregateFunctionsStatisticsCovar(AggregateFunctionFactory & factory)
{
    factory.registerFunction("covarSamp", createAggregateFunctionStatisticsBinary<AggregateFunctionCovar, StatisticsFunctionKind::covarSamp>);
    factory.registerFunction("covarPop", createAggregateFunctionStatisticsBinary<AggregateFunctionCovar, StatisticsFunctionKind::covarPop>);

    /// Synonyms for compatibility.
    factory.registerAlias("COVAR_SAMP", "covarSamp", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("COVAR_POP", "covarPop", AggregateFunctionFactory::Case::Insensitive);
}

}
