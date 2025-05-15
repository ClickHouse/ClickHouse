#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/logger_useful.h>

namespace DB
{

BackgroundSchedulePoolTaskHolder::BackgroundSchedulePoolTaskHolder() = default;
BackgroundSchedulePoolTaskHolder::BackgroundSchedulePoolTaskHolder(BackgroundSchedulePoolTaskHolder && other) noexcept = default;
BackgroundSchedulePoolTaskHolder & BackgroundSchedulePoolTaskHolder::operator=(BackgroundSchedulePoolTaskHolder && other) noexcept = default;

BackgroundSchedulePoolTaskHolder::BackgroundSchedulePoolTaskHolder(const BackgroundSchedulePoolTaskInfoPtr & task_info_) :
    task_info(task_info_)
{
    auto locked_task_info = lock();
    bool is_expired = task_info.expired();
    bool is_null = locked_task_info == nullptr;

    LOG_INFO(getLogger("BackgroundSchedulePoolTaskInfo"),
             "Constructing BackgroundSchedulePoolTaskHolder: task_info is expired? {}, use count: {}, is null: {}",
             is_expired, is_null ? 0 : locked_task_info.use_count(), is_null);
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
    auto locked_task_info = lock();
    bool is_expired = task_info.expired();
    bool is_null = locked_task_info == nullptr;

    LOG_INFO(getLogger("BackgroundSchedulePoolTaskInfo"),
             "Deconstructing BackgroundSchedulePoolTaskHolder: task_info is expired? {}, use count: {}, is null: {}",
             is_expired, is_null ? 0 : locked_task_info.use_count(), is_null);
    if (auto task = lock())
        task->deactivate();
}

BackgroundSchedulePoolTaskHolder::operator bool() const
{
    return !task_info.expired();
}

BackgroundSchedulePoolTaskInfo * BackgroundSchedulePoolTaskHolder::operator->()
{
    auto locked_task_info = lock();
    bool is_expired = task_info.expired();
    bool is_null = locked_task_info == nullptr;

    LOG_INFO(getLogger("BackgroundSchedulePoolTaskInfo"),
             "BackgroundSchedulePoolTaskHolder operator operator->(): task_info is expired? {}, use count: {}, is null: {}",
             is_expired, is_null ? 0 : locked_task_info.use_count(), is_null);

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
    auto locked_task_info = lock();
    bool is_expired = task_info.expired();
    bool is_null = locked_task_info == nullptr;

    LOG_INFO(getLogger("BackgroundSchedulePoolTaskInfo"),
             "BackgroundSchedulePoolTaskHolder operator operator->() const: task_info is expired? {}, use count: {}, is null: {}",
             is_expired, is_null ? 0 : locked_task_info.use_count(), is_null);

    assert(!task_info.expired());
    return lock().get();
}

std::shared_ptr<BackgroundSchedulePoolTaskInfo> BackgroundSchedulePoolTaskHolder::lock() const { return task_info.lock(); }

}
