#pragma once

#include <WindowFunctions/IAggregateWindowFunction.h>

namespace DB
{

AggregateWindowFunctionPtr tryCreateAggregateWindowFunctionSliding(const AggregateFunctionPtr & function, const WindowDescription & window);

}
