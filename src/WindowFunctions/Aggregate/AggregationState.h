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

class AggregationState
{
public:
    explicit AggregationState(AggregateFunctionPtr function_);
    ~AggregationState();

    void recreate();
    void add(const WindowArguments & arguments, Int64 begin, Int64 end);
    void merge(ConstAggregateDataPtr rhs);
    void insertResultInto(IColumn & result);

    [[nodiscard]] ConstAggregateDataPtr getData() const;

private:
    const AggregateFunctionPtr function;
    const bool is_state;

    AlignedBuffer place;
    std::unique_ptr<Arena> arena;
    bool created = false;

    std::function<void()> check_cancellation;
    CancellationBudget budget;
};

}
