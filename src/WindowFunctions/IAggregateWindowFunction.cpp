#include <WindowFunctions/IAggregateWindowFunction.h>
#include <WindowFunctions/Aggregate/AggregateWindowFunctionPrefix.h>
#include <WindowFunctions/Aggregate/AggregateWindowFunctionRecalculate.h>
#include <WindowFunctions/Aggregate/AggregateWindowFunctionSliding.h>

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
    if (auto prefix = tryCreateAggregateWindowFunctionPrefix(function, window))
        return prefix;
    else if (auto sliding = tryCreateAggregateWindowFunctionSliding(function, window))
        return sliding;
    else if (auto recalculate = tryCreateAggregateWindowFunctionRecalculate(function, window))
        return recalculate;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No aggregate window function for aggregate function {}", function->getName());
}

}
