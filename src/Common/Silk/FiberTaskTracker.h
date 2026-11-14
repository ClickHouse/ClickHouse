#pragma once

#include "config.h"

#if USE_SILK

#include <Common/ThreadGroupSwitcher.h>

#include <silk/fibers/future.h>

#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <vector>

namespace Silk
{

/// Runs tasks in fibers, at most `max_inflight` at once (0 means up to `max_tasks`), for a waiting fiber or thread.
/// The interface is a subset of the one of `DB::TaskTracker`: `add`, `waitAll` and `safeWaitAll`.
///
/// All tracking state is allocated in the constructor, so that a caller can allocate it before it starts work
/// that has to be undone on failure. Only started tasks are tracked: when a task cannot start, `add` throws,
/// and the started tasks are still waited for. The destructor waits for every started task.
///
/// A task runs attached to `thread_group`. Silk signals the future before it destroys the fiber, so a task
/// releases its callback and the group reference itself before it finishes.
class FiberTaskTracker
{
public:
    using Callback = std::function<void()>;

    FiberTaskTracker(size_t max_tasks, size_t max_inflight, DB::ThreadGroupPtr thread_group_);
    ~FiberTaskTracker();

    FiberTaskTracker(const FiberTaskTracker &) = delete;
    FiberTaskTracker & operator=(const FiberTaskTracker &) = delete;

    /// Starts `callback` in a fiber. When every slot is busy, first waits for any task to finish
    /// and rethrows its exception.
    void add(Callback && callback);

    /// Waits for every started task and rethrows the first exception it observes. `safeWaitAll` waits for the rest.
    void waitAll();

    /// Waits for every started task. Their exceptions are logged and not propagated.
    void safeWaitAll() noexcept;

private:
    struct Slot
    {
        silk::FiberFuture future;
        Callback callback;
        DB::ThreadGroupPtr thread_group;
        std::exception_ptr exception;
        bool started = false;
    };

    static int runTask(Slot & slot) noexcept;

    /// Waits for the started task in the slot and frees the slot. Returns the exception of the task, if any.
    std::exception_ptr finish(size_t index);

    const DB::ThreadGroupPtr thread_group;
    const size_t slot_count;
    const std::unique_ptr<Slot[]> slots;
    /// The futures of all slots, for `silk::FiberFuture::waitForMultiple`.
    std::vector<silk::FiberFuture *> futures;
    std::vector<size_t> free_slots;
};

}

#endif
