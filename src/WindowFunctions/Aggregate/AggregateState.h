#pragma once

#include <AggregateFunctions/IAggregateFunction_fwd.h>

#include <Interpreters/CancellationBudget.h>

#include <Columns/IColumn_fwd.h>

#include <Common/AlignedBuffer.h>

#include <base/defines.h>
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
    void insertResultInto(IColumn & result);

private:
    /// Called for every evaluated row, so only the cheap test is inlined.
    ALWAYS_INLINE void checkCreated() const
    {
        if (unlikely(!created))
            throwNotCreated();
    }

    [[noreturn]] NO_INLINE void throwNotCreated() const;

    AggregateFunctionPtr function;
    AlignedBuffer place;
    std::unique_ptr<Arena> arena;
    const bool is_state;
    std::function<void()> check_cancellation;
    CancellationBudget budget;
    /// False after `destroy` until `create` succeeds, so a throwing `create` in `recreate` does not lead to a second `destroy`.
    bool created = false;
};

}
