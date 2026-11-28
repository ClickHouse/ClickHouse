#pragma once

#include <WindowFunctions/IAggregateWindowFunction.h>

namespace DB
{

AggregateWindowFunctionPtr tryCreateAggregateWindowFunctionPrefix(const AggregateFunctionPtr & function, const WindowDescription & window);

}
