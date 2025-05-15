#pragma once

#include <memory>

namespace DB
{

class BackgroundSchedulePoolTaskInfo;

using BackgroundSchedulePoolTaskInfoPtr = std::shared_ptr<BackgroundSchedulePoolTaskInfo>;
using BackgroundSchedulePoolTaskInfoWeakPtr = std::weak_ptr<BackgroundSchedulePoolTaskInfo>;


class BackgroundSchedulePoolTaskHolder
{
public:
    BackgroundSchedulePoolTaskHolder();
    explicit BackgroundSchedulePoolTaskHolder(const BackgroundSchedulePoolTaskInfoPtr & task_info_);
    BackgroundSchedulePoolTaskHolder(const BackgroundSchedulePoolTaskHolder & other) = delete;
    BackgroundSchedulePoolTaskHolder(BackgroundSchedulePoolTaskHolder && other) noexcept;
    BackgroundSchedulePoolTaskHolder & operator=(const BackgroundSchedulePoolTaskHolder & other) noexcept = delete;
    BackgroundSchedulePoolTaskHolder & operator=(BackgroundSchedulePoolTaskHolder && other) noexcept;

    ~BackgroundSchedulePoolTaskHolder();

    explicit operator bool() const;

    BackgroundSchedulePoolTaskInfo * operator->();
    const BackgroundSchedulePoolTaskInfo * operator->() const;

private:
    // The task is exclusively owned by the BackgroundSchedulePool via strong references (shared_ptr).
    // Task holders hold only weak references (weak_ptr) to avoid extending the task's lifetime.
    BackgroundSchedulePoolTaskInfoWeakPtr task_info;

    std::shared_ptr<BackgroundSchedulePoolTaskInfo> lock() const;
};

}
