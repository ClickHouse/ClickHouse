#pragma once

#include <memory>
#include <vector>

namespace DB
{
using AggregateDataPtr = char *;
using AggregateDataPtrs = std::vector<AggregateDataPtr>;
using ConstAggregateDataPtr = const char *;

class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<const IAggregateFunction>;
}
