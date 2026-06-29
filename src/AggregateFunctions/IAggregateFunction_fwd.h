#pragma once

#include <memory>

#include <Common/VectorWithMemoryTracking.h>

namespace DB
{
using AggregateDataPtr = char *;
using AggregateDataPtrs = VectorWithMemoryTracking<AggregateDataPtr>;
using ConstAggregateDataPtr = const char *;

class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<const IAggregateFunction>;
}
