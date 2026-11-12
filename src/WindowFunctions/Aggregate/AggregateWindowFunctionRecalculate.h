#pragma once

#include <WindowFunctions/Aggregate/AggregateState.h>
#include <WindowFunctions/IAggregateWindowFunction.h>

namespace DB
{

class AggregateWindowFunctionRecalculate final : public IAggregateWindowFunction
{
public:
    explicit AggregateWindowFunctionRecalculate(AggregateFunctionPtr function);

    void resetPartition() override;
    Int64 evaluate(const WindowBounds & bounds, const WindowArguments & arguments, IColumn & result) override;

private:
    AggregateState state;
    Int64 covered_start = 0;
    Int64 covered_end = 0;
};

}
