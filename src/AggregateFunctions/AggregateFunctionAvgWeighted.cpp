#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{
void registerAggregateFunctionAvgWeighted(AggregateFunctionFactory &)
{
    //"create function avgWeighted as (x, w) -> sum(x * w) / sum(w)";
}
}
