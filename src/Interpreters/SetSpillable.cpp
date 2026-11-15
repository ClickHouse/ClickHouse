#include <Interpreters/SetSpillable.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnSet.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Set.h>

#include <algorithm>

namespace DB
{

void SetSpillState::bind(std::shared_ptr<Set> set_)
{
    chassert(!bound.load(std::memory_order_relaxed));
    set = std::move(set_);
    bound.store(true, std::memory_order_release);
}

Set * SetSpillState::getSet() const
{
    return bound.load(std::memory_order_acquire) ? set.get() : nullptr;
}

ProcessorMemoryStats SetSpillState::getMemoryStats() const
{
    auto * current = getSet();
    return current ? current->getMemoryStats() : ProcessorMemoryStats{};
}

size_t SetSpillState::spill(size_t at_least_bytes)
{
    auto * current = getSet();
    return current ? current->spill(at_least_bytes) : 0;
}

const TemporaryDataOnDiskScope * SetSpillState::getSpillScope() const
{
    auto * current = getSet();
    return current ? current->getSpillScope() : nullptr;
}

void SetSpillables::add(const std::shared_ptr<SetSpillState> & state)
{
    if (std::ranges::find(states, state) != states.end())
        return;
    states.push_back(state);
    components.push_back(state->getSpillable());
}

void SetSpillables::add(const IColumn & column)
{
    if (const auto * constant = typeid_cast<const ColumnConst *>(&column))
        add(constant->getDataColumn());
    else if (const auto * column_set = typeid_cast<const ColumnSet *>(&column))
    {
        if (const auto * future = typeid_cast<const FutureSetFromSubquery *>(column_set->getData().get()))
            add(future->getSetAndKey()->spill_state);
    }
    else if (const auto * function = typeid_cast<const ColumnFunction *>(&column))
    {
        if (const auto * expression = typeid_cast<const FunctionExpression *>(function->getFunction().get()))
            add(expression->getAcionsDAG());
        for (const auto & captured : function->getCapturedColumns())
            if (captured.column)
                add(*captured.column);
    }
}

void SetSpillables::add(const ActionsDAG & actions)
{
    for (const auto & node : actions.getNodes())
    {
        if (node.column)
            add(*node.column);
        if (const auto * capture = typeid_cast<const FunctionCapture *>(node.function_base.get()))
            add(capture->getAcionsDAG());
    }
}

}
