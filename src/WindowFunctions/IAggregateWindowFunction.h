#pragma once

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Columns/IColumn_fwd.h>
#include <base/types.h>

#include <memory>

namespace DB
{

struct WindowBounds;
struct WindowDescription;
class WindowArguments;

class IAggregateWindowFunction
{
public:
    virtual ~IAggregateWindowFunction() = default;

    virtual void resetPartition() = 0;
    virtual Int64 evaluate(const WindowBounds & bounds, const WindowArguments & arguments, IColumn & result) = 0;
};

using AggregateWindowFunctionPtr = std::shared_ptr<IAggregateWindowFunction>;

AggregateWindowFunctionPtr createAggregateWindowFunction(AggregateFunctionPtr function, const WindowDescription & window);

}
