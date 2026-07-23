#pragma once

#include "config.h"

#if USE_SILK

#include <Common/ThreadGroupSwitcher.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <cstddef>
#include <type_traits>
#include <utility>

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

/// The one sanctioned spawn point for fibers on the server-wide scheduler:
/// enforces at compile time that the parameters begin with SilkFiberJobHeader,
/// which the global fiber-switch hooks blind-cast to.
template <typename T>
[[nodiscard]] int runSilkFiber(int (*fiber_main)(T *) noexcept, T && parameters, uint8_t category, silk::FiberFuture * future)
{
    static_assert(offsetof(T, header) == 0, "fiber params must begin with SilkFiberJobHeader");
    static_assert(std::is_same_v<decltype(T::header), SilkFiberJobHeader>);
    return silk::FiberScheduler::run(fiber_main, std::move(parameters), category, future);
}

}

#endif
