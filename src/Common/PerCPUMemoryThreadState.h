#pragma once

#include <base/types.h>

#include <utility>

namespace DB
{

/// Per-thread state for the per-CPU untracked-memory bound (see PerCPUMemory). Touched only by its
/// owning thread, so it needs no synchronization.
struct PerCPUMemoryThreadState
{
    /// Signed bytes this thread has added on contributed_on_cpu; the sign selects the side
    /// (allocated when > 0, freed when < 0).
    Int64 contributed = 0;
    /// CPU whose counter holds `contributed` (-1 when nothing is contributed).
    int contributed_on_cpu = -1;

    /// Detach the contribution for a nested tracker scope (MemoryTrackerSwitcher): resets this but
    /// leaves the bytes in the CPU counter (restored later). The && qualifier forces the caller to
    /// move, so the reset is visible at the call site.
    [[nodiscard]] PerCPUMemoryThreadState save() &&
    {
        PerCPUMemoryThreadState saved;
        std::swap(*this, saved);
        return saved;
    }

    void restore(const PerCPUMemoryThreadState & saved)
    {
        *this = saved;
    }
};

}
