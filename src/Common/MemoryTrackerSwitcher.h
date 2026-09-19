#pragma once

#include <Common/MemoryTracker.h>

namespace DB
{

struct MemoryTrackerSwitcher
{
    explicit MemoryTrackerSwitcher(MemoryTracker * new_tracker);
    ~MemoryTrackerSwitcher();

private:
    MemoryTracker * prev_memory_tracker_parent = nullptr;
    Int64 prev_untracked_memory = 0;
    VariableContext prev_untracked_memory_blocker_level = VariableContext::Max;
};

}
