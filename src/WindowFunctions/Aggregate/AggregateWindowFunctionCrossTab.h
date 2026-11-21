#pragma once

#include <WindowFunctions/IAggregateWindowFunction.h>

namespace DB
{

AggregateWindowFunctionPtr tryCreateAggregateWindowFunctionCrossTab(const AggregateFunctionPtr & function, const WindowDescription & window);

}
