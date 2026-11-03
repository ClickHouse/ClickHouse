#include <Interpreters/SetSpillable.h>

#include <Interpreters/PreparedSets.h>
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

void SetSpillables::add(const ActionsDAG & actions)
{
    forEachFutureSet(actions, [&](const FutureSetPtr & set)
    {
        if (const auto * future = typeid_cast<const FutureSetFromSubquery *>(set.get()))
            add(future->getSetAndKey()->spill_state);
    });
}

}
