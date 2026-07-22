#pragma once

#include "config.h"

#if USE_SILK

#include <Common/ThreadGroupSwitcher.h>

namespace DB
{

/// Convention for fibers spawned on the server-wide Silk scheduler: the
/// fiber's parameters struct MUST begin with this header. The global
/// fiber-switch hooks read it via `FiberScheduler::getFiberParameters` on
/// every fiber to attach/detach the submitter's `ThreadGroup` on the OS
/// thread the fiber borrows (memory accounting, per-user throttling).
struct SilkFiberJobHeader
{
    ThreadGroupPtr thread_group;
};

namespace SilkFiberCategory
{
    /// Reader executor fetch steps.
    inline constexpr uint8_t FETCH = 1;
}

}

#endif
