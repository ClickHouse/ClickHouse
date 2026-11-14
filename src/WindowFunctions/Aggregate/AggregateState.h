#pragma once

#include <AggregateFunctions/IAggregateFunction_fwd.h>

#include <Functions/CancellationBudget.h>

#include <Columns/IColumn_fwd.h>

#include <Common/AlignedBuffer.h>

#include <base/types.h>

#include <functional>
#include <memory>

namespace DB
{

class Arena;
class WindowArguments;

class AggregateState
{
public:
    explicit AggregateState(AggregateFunctionPtr function_);
    ~AggregateState();

    void recreate();
    void add(const WindowArguments & arguments, Int64 begin, Int64 end);
    /// `frame_rows` is the number of rows in the state: finalization can cost as much as all of them
    /// (e.g. `quantileExact` selects over them), so it is charged to the cancellation budget too.
    void insertResultInto(IColumn & result, Int64 frame_rows);

private:
    AggregateFunctionPtr function;
    AlignedBuffer place;
    std::unique_ptr<Arena> arena;
    const bool is_state;
    std::function<void()> check_cancellation;
    CancellationBudget budget;
};

}
