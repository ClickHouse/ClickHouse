#pragma once

#include <memory>

namespace DB
{

class BackgroundSchedulePoolTaskInfo;
class BackgroundSchedulePoolTaskHolder;

using BackgroundSchedulePoolTaskInfoPtr = std::shared_ptr<BackgroundSchedulePoolTaskInfo>;

class BackgroundSchedulePoolTaskHolder
{
public:
    BackgroundSchedulePoolTaskHolder() = default;
    explicit BackgroundSchedulePoolTaskHolder(const BackgroundSchedulePoolTaskInfoPtr & task_info_) : task_info(task_info_) {}
    BackgroundSchedulePoolTaskHolder(const BackgroundSchedulePoolTaskHolder & other) = delete;
    BackgroundSchedulePoolTaskHolder(BackgroundSchedulePoolTaskHolder && other) noexcept = default;
    BackgroundSchedulePoolTaskHolder & operator=(const BackgroundSchedulePoolTaskHolder & other) noexcept = delete;
    BackgroundSchedulePoolTaskHolder & operator=(BackgroundSchedulePoolTaskHolder && other) noexcept = default;

    ~BackgroundSchedulePoolTaskHolder();

    explicit operator bool() const { return task_info != nullptr; }

    BackgroundSchedulePoolTaskInfo * operator->() { return task_info.get(); }
    const BackgroundSchedulePoolTaskInfo * operator->() const { return task_info.get(); }

private:
    BackgroundSchedulePoolTaskInfoPtr task_info;
};

}
