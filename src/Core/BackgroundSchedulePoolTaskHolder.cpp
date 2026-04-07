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

}
