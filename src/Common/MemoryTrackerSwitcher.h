#pragma once

#include <Common/MemoryTracker.h>
#include <Common/PerCPUMemoryThreadState.h>

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
    /// The per-CPU contribution accounts for the same un-flushed bytes as untracked_memory,
    /// so it must be saved/restored together with it; otherwise allocations in the switched
    /// scope fold into the previous parent's contribution.
    PerCPUMemoryThreadState prev_per_cpu;
    /// The cached sample config follows the tracker parent, so re-resolve it for the switched
    /// scope and restore it after (see ThreadStatus::resolveMemorySampleConfig).
    MemoryTracker::SampleConfig prev_sample_config;
};

}
