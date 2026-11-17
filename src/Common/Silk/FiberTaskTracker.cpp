#include <Common/Silk/FiberTaskTracker.h>

#if USE_SILK

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Silk/FiberScheduler.h>
#include <Common/scope_guard_safe.h>
#include <base/defines.h>
#include <base/errnoToString.h>

#include <algorithm>
#include <cerrno>
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

FiberTaskTracker::FiberTaskTracker(size_t max_tasks, size_t max_inflight, DB::ThreadGroupPtr thread_group_)
    : thread_group(std::move(thread_group_))
    , slot_count(max_inflight == 0 ? max_tasks : std::min(max_tasks, max_inflight))
    , slots(std::make_unique<Slot[]>(slot_count))
{
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
    return 0;
}

void FiberTaskTracker::add(Callback && callback)
{
    if (free_slots.empty())
    {
        /// Every slot is busy, so every future belongs to a started task.
        const size_t index = silk::FiberFuture::waitForMultiple(futures.data(), futures.size());
        if (auto exception = finish(index))
            std::rethrow_exception(exception);
    }

    const size_t index = free_slots.back();
    Slot & slot = slots[index];
    slot.callback = std::move(callback);
    slot.thread_group = thread_group;

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
        throw;
    }

    if (result != 0)
    {
        slot.callback = {};
        slot.thread_group.reset();
        throw DB::Exception(DB::ErrorCodes::CANNOT_SCHEDULE_TASK, "Cannot start a fiber: {}", errnoToString(result));
    }

    slot.started = true;
    free_slots.pop_back();
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
