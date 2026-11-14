#pragma once

#include <base/types.h>

#include <vector>

namespace DB
{

/// Maps the CPUs in the process affinity mask, which can be sparse (e.g. 5, 17, 40), to consecutive slots (0, 1, 2),
/// so that a per-CPU pool has one entry per allowed CPU and uses all of them. The mask is read once, at construction.
class PerCPUSlots
{
public:
    PerCPUSlots();

    /// Number of allowed CPUs, or zero if the affinity mask could not be read.
    size_t size() const { return num_slots; }

    /// Slot of the CPU the calling thread runs on, or zero if that CPU is unknown or was not allowed at construction.
    size_t getCurrentSlot() const;

private:
    std::vector<UInt32> slot_by_cpu;
    size_t num_slots = 0;
};

}
