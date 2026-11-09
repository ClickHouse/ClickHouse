#pragma once

#include <WindowFunctions/Aggregate/AggregateState.h>
#include <WindowFunctions/IAggregateWindowFunction.h>

namespace DB
{

class AggregateWindowFunctionPrefix final : public IAggregateWindowFunction
{
public:
    explicit AggregateWindowFunctionPrefix(AggregateFunctionPtr function);

    void resetPartition() override;
    Int64 evaluate(const WindowBounds & bounds, const WindowArguments & arguments, IColumn & result) override;

private:
    AggregateState state;
    Int64 covered_end = 0;
};

}
