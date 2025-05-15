#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{

BackgroundSchedulePoolTaskHolder::BackgroundSchedulePoolTaskHolder() = default;
BackgroundSchedulePoolTaskHolder::BackgroundSchedulePoolTaskHolder(BackgroundSchedulePoolTaskHolder && other) noexcept = default;
BackgroundSchedulePoolTaskHolder & BackgroundSchedulePoolTaskHolder::operator=(BackgroundSchedulePoolTaskHolder && other) noexcept = default;

BackgroundSchedulePoolTaskHolder::BackgroundSchedulePoolTaskHolder(const BackgroundSchedulePoolTaskInfoPtr & task_info_) :
    task_info(task_info_)
{
}

/**
 * The task is weakly referenced by TaskHolder but strongly owned by the pool.
 * If the pool has already been destroyed, all its tasks are destroyed as well,
 * so there's no need to call deactivate().
 *
 * Here, we try to lock the weak pointer to get a shared_ptr.
 * If successful, it means the pool (and task) still exist, so we call deactivate().
 */
BackgroundSchedulePoolTaskHolder::~BackgroundSchedulePoolTaskHolder()
{
    if (auto task = lock())
        task->deactivate();
}

BackgroundSchedulePoolTaskHolder::operator bool() const
{
    return !task_info.expired();
}

BackgroundSchedulePoolTaskInfo * BackgroundSchedulePoolTaskHolder::operator->()
{
    /**
     * Assert that task_info is not expired because:
     * - The BackgroundSchedulePool owns the TaskInfo with a strong shared_ptr.
     * - As long as the pool exists, task_info (a weak_ptr) must be valid.
     * - The pool is only destroyed during system shutdown.
     *   During shutdown, this assert may fail, but such failures can be safely ignored
     *   since no further task usage is expected.
     *
     * Therefore, under normal running conditions, this operator assumes task_info is valid
     * and calling lock().get() is safe.
     */
    assert(!task_info.expired());
    return lock().get();
}

const BackgroundSchedulePoolTaskInfo * BackgroundSchedulePoolTaskHolder::operator->() const
{
    assert(!task_info.expired());
    return lock().get();
}

std::shared_ptr<BackgroundSchedulePoolTaskInfo> BackgroundSchedulePoolTaskHolder::lock() const { return task_info.lock(); }

}
