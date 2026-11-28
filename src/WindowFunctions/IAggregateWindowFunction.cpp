#include <WindowFunctions/IAggregateWindowFunction.h>
#include <WindowFunctions/Aggregate/AggregateWindowFunctionCrossTab.h>
#include <WindowFunctions/Aggregate/AggregateWindowFunctionPrefix.h>
#include <WindowFunctions/Aggregate/AggregateWindowFunctionRecalculate.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

AggregateWindowFunctionPtr createAggregateWindowFunction(AggregateFunctionPtr function, const WindowDescription & window)
{
    if (auto cross_tab = tryCreateAggregateWindowFunctionCrossTab(function, window))
        return cross_tab;
    else if (auto prefix = tryCreateAggregateWindowFunctionPrefix(function, window))
        return prefix;
    else if (auto recalculate = tryCreateAggregateWindowFunctionRecalculate(function, window))
        return recalculate;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No aggregate window function for aggregate function {}", function->getName());
}

}
