#pragma once

#include <memory>
#include <vector>

namespace DB
{
using AggregateDataPtr = char *;
using AggregateDataPtrs = std::vector<AggregateDataPtr>; // STYLE_CHECK_ALLOW_STD_CONTAINERS
using ConstAggregateDataPtr = const char *;

class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<const IAggregateFunction>;
}
