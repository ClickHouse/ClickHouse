#pragma once

#include <memory>
#include <mutex>

namespace DB
{

class BackgroundSchedulePoolTaskInfo;

using BackgroundSchedulePoolTaskInfoPtr = std::shared_ptr<BackgroundSchedulePoolTaskInfo>;

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

    /// Get the shared pointer to the task info.
    /// Useful when you need to extend the lifetime of the task.
    BackgroundSchedulePoolTaskInfoPtr getTaskInfoPtr() const;

private:
    BackgroundSchedulePoolTaskInfoPtr task_info;
};

/// RAII guard that pauses parts check and reactivates it on destruction.
/// Safe to destroy from any thread.
class PausableTask
{
public:
    explicit PausableTask(BackgroundSchedulePoolTaskHolder task_);

    PausableTask(const PausableTask &) = delete;
    PausableTask & operator=(const PausableTask &) = delete;
    PausableTask(PausableTask &&) = delete;
    PausableTask & operator=(PausableTask &&) = delete;

    BackgroundSchedulePoolTaskHolder & getTask();

    void pause();
    void resume();

    struct PauseHolder
    {
        explicit PauseHolder(PausableTask & task_);
        ~PauseHolder();

        private:
            PausableTask & task;
    };

    using PauseHolderPtr = std::unique_ptr<PauseHolder>;
private:
    std::mutex pause_mutex;
    size_t pause_count = 0;
    BackgroundSchedulePoolTaskHolder task;
};

}
