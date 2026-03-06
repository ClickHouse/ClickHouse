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

BackgroundSchedulePoolTaskHolder::~BackgroundSchedulePoolTaskHolder()
{
    if (task_info)
        task_info->deactivate();
}

BackgroundSchedulePoolTaskHolder::operator bool() const
{
    return task_info != nullptr;
}

BackgroundSchedulePoolTaskInfo * BackgroundSchedulePoolTaskHolder::operator->()
{
    return task_info.get();
}

const BackgroundSchedulePoolTaskInfo * BackgroundSchedulePoolTaskHolder::operator->() const
{
    return task_info.get();
}

BackgroundSchedulePoolTaskInfoPtr BackgroundSchedulePoolTaskHolder::getTaskInfoPtr() const
{
    return task_info;
}

BackgroundSchedulePoolTaskHolder & BackgroundSchedulePoolPausableTask::getTask()
{
    return task;
}

BackgroundSchedulePoolPausableTask::BackgroundSchedulePoolPausableTask(BackgroundSchedulePoolTaskHolder task_)
    : task(std::move(task_))
{
}

BackgroundSchedulePoolPausableTask::PauseHolderPtr BackgroundSchedulePoolPausableTask::pause()
{
    return std::make_unique<PauseHolder>(*this);
}

void BackgroundSchedulePoolPausableTask::pauseImpl()
{
    std::lock_guard lock(pause_mutex);
    pause_count++;
    if (pause_count == 1)
        task->deactivate();
}

void BackgroundSchedulePoolPausableTask::resumeImpl()
{
    std::lock_guard lock(pause_mutex);
    pause_count--;
    if (pause_count == 0)
        task->activateAndSchedule();
}

BackgroundSchedulePoolPausableTask::PauseHolder::PauseHolder(BackgroundSchedulePoolPausableTask & task_)
    : task(task_)
{
    task.pauseImpl();
}

BackgroundSchedulePoolPausableTask::PauseHolder::~PauseHolder()
{
    task.resumeImpl();
}

}
