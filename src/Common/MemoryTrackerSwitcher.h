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
    /// The cached sample config follows the tracker parent, so re-resolve it for the switched
    /// scope and restore it after (see ThreadStatus::resolveMemorySampleConfig).
    MemoryTracker::SampleConfig prev_sample_config;
};

}
