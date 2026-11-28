#pragma once

#include "config.h"

#if USE_SILK

#include <Common/Silk/FiberSemaphore.h>
#include <Common/ThreadGroupSwitcher.h>

#include <silk/fibers/future.h>

#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
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
/// A task runs attached to `thread_group`, and under a `DB::ThreadStatus::QueryCancellationBlocker` if `add` was
/// called under one, as a `ThreadPool` job does. Silk signals the future before it destroys the fiber, so a task
/// releases its callback and the group reference itself before it finishes.
///
/// With `limit`, every task also holds a permit of that semaphore, which bounds the tasks of all trackers that share it.
/// `add` takes the permit before it starts the fiber, so a caller waits instead of starting fibers that would only wait,
/// and such a tracker is used in a fiber. A fiber that ends before running its task returns its permit only when the
/// caller waits for it, so while tasks of the tracker are started, `add` waits for them rather than for a permit, and
/// rethrows their exceptions. A caller that also drives other trackers of the same limit may still wait for the permits
/// of their fibers that ended before running their tasks.
class FiberTaskTracker
{
public:
    using Callback = std::function<void()>;

    FiberTaskTracker(size_t max_tasks, size_t max_inflight, DB::ThreadGroupPtr thread_group_, FiberSemaphore * limit_ = nullptr);
    ~FiberTaskTracker();

    FiberTaskTracker(const FiberTaskTracker &) = delete;
    FiberTaskTracker & operator=(const FiberTaskTracker &) = delete;

    /// Starts `callback` in a fiber. When every slot is busy, or with `limit` no permit is free, first waits for any task
    /// to finish and rethrows its exception.
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
        bool query_cancellation_blocked = false;
        bool started = false;
        /// A permit of `limit`, from `add` until the task ends, or `finish` if the task never ran.
        std::optional<FiberSemaphore::Permit> permit;
    };

    static int runTask(Slot & slot) noexcept;

    /// Waits for any started task, frees its slot and rethrows its exception.
    void waitForAnyTask();

    /// Waits for a free permit of `limit`, or for started tasks of this tracker while they hold permits.
    FiberSemaphore::Permit takePermit();

    /// Waits for the started task in the slot and frees the slot. Returns the exception of the task, if any.
    std::exception_ptr finish(size_t index);

    const DB::ThreadGroupPtr thread_group;
    FiberSemaphore * const limit;
    const size_t slot_count;
    const std::unique_ptr<Slot[]> slots;
    /// The futures of all slots, for `silk::FiberFuture::waitForMultiple`.
    std::vector<silk::FiberFuture *> futures;
    std::vector<size_t> free_slots;
};

}

#endif
