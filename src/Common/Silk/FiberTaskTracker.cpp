#include <Common/Silk/FiberTaskTracker.h>

#if USE_SILK

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Silk/FiberScheduler.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>
#include <base/defines.h>
#include <base/errnoToString.h>

#include <algorithm>
#include <cerrno>
#include <optional>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SCHEDULE_TASK;
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char silk_fiber_task_tracker_spawn_failure[];
}

}

namespace Silk
{

FiberTaskTracker::FiberTaskTracker(size_t max_tasks, size_t max_inflight, DB::ThreadGroupPtr thread_group_, FiberSemaphore * limit_)
    : thread_group(std::move(thread_group_))
    , limit(limit_)
    , slot_count(max_inflight == 0 ? max_tasks : std::min(max_tasks, max_inflight))
    , slots(std::make_unique<Slot[]>(slot_count))
{
    /// The permits are taken and released through fiber locks.
    chassert(!limit || isInsideFiber());

    if (slot_count == 0)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "FiberTaskTracker needs at least one slot, got max_tasks = 0");

    futures.reserve(slot_count);
    free_slots.reserve(slot_count);
    for (size_t index = 0; index < slot_count; ++index)
    {
        futures.push_back(&slots[index].future);
        free_slots.push_back(slot_count - 1 - index);
    }
}

FiberTaskTracker::~FiberTaskTracker()
{
    safeWaitAll();
}

int FiberTaskTracker::runTask(Slot & slot) noexcept
{
    try
    {
        std::optional<DB::ThreadStatus::QueryCancellationBlocker> cancellation_blocker;
        if (slot.query_cancellation_blocked)
            cancellation_blocker.emplace();
        /// Fibers are not renamed, so the thread name is not used.
        DB::ThreadGroupSwitcher switcher(std::move(slot.thread_group), DB::ThreadName::UNKNOWN);
        /// Released under the group and before the waiter is signaled, as in `threadPoolCallbackRunner.h`.
        SCOPE_EXIT_SAFE({ [[maybe_unused]] auto released = std::move(slot.callback); });
        slot.callback();
    }
    catch (...)
    {
        slot.exception = std::current_exception();
    }
    /// Before the waiter is signaled, so that a waiting `add` of any tracker can start its task.
    slot.permit.reset();
    return 0;
}

void FiberTaskTracker::add(Callback && callback)
{
    if (free_slots.empty())
        waitForAnyTask();

    /// Taken before the slot is chosen: waiting for a permit may finish a task and free its slot.
    std::optional<FiberSemaphore::Permit> permit;
    if (limit)
        permit.emplace(takePermit());

    const size_t index = free_slots.back();
    Slot & slot = slots[index];

    if (permit)
        slot.permit.emplace(std::move(*permit));

    slot.callback = std::move(callback);
    slot.thread_group = thread_group;
    slot.query_cancellation_blocked = DB::ThreadStatus::QueryCancellationBlocker::isActive();

    int result = 0;
    try
    {
        /// Fails as `Silk::spawn` does when it cannot allocate a fiber.
        fiu_do_on(DB::FailPoints::silk_fiber_task_tracker_spawn_failure, { result = ENOMEM; });
        if (result == 0)
            result = spawn([task_slot = &slot] { return runTask(*task_slot); }, slot.future);
    }
    catch (...)
    {
        slot.callback = {};
        slot.thread_group.reset();
        slot.permit.reset();
        throw;
    }

    if (result != 0)
    {
        slot.callback = {};
        slot.thread_group.reset();
        slot.permit.reset();
        throw DB::Exception(DB::ErrorCodes::CANNOT_SCHEDULE_TASK, "Cannot start a fiber: {}", errnoToString(result));
    }

    slot.started = true;
    free_slots.pop_back();
}

void FiberTaskTracker::waitForAnyTask()
{
    chassert(free_slots.size() < slot_count);

    /// The futures of free slots are not set, so the future that is set belongs to a started task.
    const size_t index = silk::FiberFuture::waitForMultiple(futures.data(), futures.size());
    if (auto exception = finish(index))
        std::rethrow_exception(exception);
}

FiberSemaphore::Permit FiberTaskTracker::takePermit()
{
    /// A fiber that ends before running its task returns its permit only in `finish`, which runs in the caller. While
    /// tasks of this tracker are started, the caller waits for them instead, as their permits may be the only ones left.
    while (free_slots.size() < slot_count)
    {
        if (auto permit = limit->tryTakePermit())
            return std::move(*permit);
        waitForAnyTask();
    }
    return FiberSemaphore::Permit(*limit);
}

std::exception_ptr FiberTaskTracker::finish(size_t index)
{
    Slot & slot = slots[index];
    chassert(slot.started);

    const int result = slot.future.wait();
    slot.future.reset();
    slot.started = false;
    /// Left in the slot if the fiber finished before running the task.
    slot.callback = {};
    slot.thread_group.reset();
    slot.permit.reset();
    free_slots.push_back(index);

    if (auto exception = std::exchange(slot.exception, nullptr))
        return exception;
    if (result != 0)
        return std::make_exception_ptr(DB::Exception(
            DB::ErrorCodes::CANNOT_SCHEDULE_TASK, "A fiber finished before running its task: {}", errnoToString(result)));
    return {};
}

void FiberTaskTracker::waitAll()
{
    for (size_t index = 0; index < slot_count; ++index)
    {
        if (!slots[index].started)
            continue;
        if (auto exception = finish(index))
            std::rethrow_exception(exception);
    }
}

void FiberTaskTracker::safeWaitAll() noexcept
{
    for (size_t index = 0; index < slot_count; ++index)
    {
        if (!slots[index].started)
            continue;
        try
        {
            if (auto exception = finish(index))
                std::rethrow_exception(exception);
        }
        catch (...)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

}

#endif
