#pragma once

#include <WindowFunctions/IAggregateWindowFunction.h>

namespace DB
{

AggregateWindowFunctionPtr tryCreateAggregateWindowFunctionRecalculate(const AggregateFunctionPtr & function, const WindowDescription & window);

}
