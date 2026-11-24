#pragma once

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Columns/IColumn_fwd.h>
#include <Common/AlignedBuffer.h>
#include <base/types.h>

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
    AggregateFunctionPtr function;
    AlignedBuffer place;
    std::unique_ptr<Arena> arena;
    const bool is_state;
};

}
