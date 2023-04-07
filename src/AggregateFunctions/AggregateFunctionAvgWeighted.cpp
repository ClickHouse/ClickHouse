#include <AggregateFunctions/AggregateFunctionFactory.h>
#include "Interpreters/executeQuery.h"

namespace DB
{
void registerAggregateFunctionAvgWeighted(AggregateFunctionFactory &)
{
    //const std::string_view udf = "create function avg as (x) -> sum(x) / count(x)";
    const std::string_view udf = "create function avgWeighted as (x, w) -> toFloat64(sum(x * w) / sum(w))";
    auto block = executeQuery(udf, getContext(), /*internal*/true);
}
}
