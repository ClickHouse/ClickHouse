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
/// fiber's parameters struct MUST begin with this header. It is the swap slot
/// the global fiber-switch hooks blind-cast to via `FiberScheduler::getFiberParameters`
/// on every fiber switch: one `std::swap` with `DB::current_thread` serves as both the
/// suspend and the resume hook (swap is its own inverse), parking the fiber's own
/// `ThreadStatus *` here while it isn't running and restoring the borrowing OS thread's
/// own `current_thread` meanwhile. The fiber's `ThreadStatus` itself is created and
/// attached to its submitter's `ThreadGroup` by the spawn site, not by this header.
struct SilkFiberJobHeader
{
    ThreadStatus * saved_current_thread = nullptr;
};

namespace SilkFiberCategory
{
    /// Reader executor fetch steps. Category 1 is taken by fibers spawned through
    /// `Silk::spawn` (`CLICKHOUSE_FIBER_CATEGORY` in SilkFiberScheduler.cpp), which follow
    /// a different parameters convention; the scheduler-wide switch hooks dispatch on the
    /// category, so the two must not collide.
    inline constexpr uint8_t FETCH = 2;
}

/// The one sanctioned spawn point for fibers on the server-wide scheduler:
/// enforces at compile time that the parameters begin with SilkFiberJobHeader,
/// which the global fiber-switch hooks blind-cast to.
template <typename T>
[[nodiscard]] int runSilkFiber(int (*fiber_main)(T *) noexcept, T && parameters, uint8_t category, silk::FiberFuture * future)
{
    static_assert(offsetof(T, header) == 0, "fiber params must begin with SilkFiberJobHeader");
    static_assert(std::is_same_v<decltype(T::header), SilkFiberJobHeader>);
    return silk::FiberScheduler::run(fiber_main, std::forward<T>(parameters), category, future);
}

}

#endif
