#pragma once

#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/Epoll.h>
#include <Common/TimerDescriptor.h>
#include <Common/WakeupFd.h>

#include <atomic>
#include <map>
#include <mutex>

namespace DB
{

namespace Runtime::V1
{

#if defined(OS_LINUX) || defined(OS_DARWIN)

/// This queue is used to poll descriptors. Generally, just a wrapper over epoll (kqueue on macOS).
class PollingQueue
{
public:
    enum class Notification : uint8_t
    {
        Finish = 1,
        Spill = 2,
    };

private:
    using Clock = std::chrono::steady_clock;
    using Key = std::uintptr_t;

    class Deadlines
    {
    public:
        void arm(Key key, int64_t timeout_ms);
        void cancel(Key key);

        [[nodiscard]] std::optional<Clock::time_point> nextDeadline() const;
        std::optional<Key> popExpired();

    private:
        using Queue = MultiMapWithMemoryTracking<Clock::time_point, Key>;
        Queue queue;
        UnorderedMapWithMemoryTracking<Key, Queue::iterator> index;
    };

    struct TaskData
    {
        size_t thread_num = 0;

        void * data = nullptr;
        int fd = -1;
        uint8_t notifications = 0;

        /// Notifications do not contain a task; inspect them with `hasNotification`.
        explicit operator bool() const { return data; }
        [[nodiscard]] bool hasNotification(Notification notification) const { return notifications & static_cast<uint8_t>(notification); }
    };

    TaskData getTask(std::unique_lock<std::mutex> & lock, int timeout);
    TaskData popExpiredDeadlineTask();
    void updateTimer();

    std::string dumpTasks() const;

public:
    PollingQueue();

    size_t size() const { return tasks.size(); }
    bool empty() const { return tasks.empty(); }

    /// Add new task to queue.
    void addTask(size_t thread_number, void * data, int fd, uint32_t events = EPOLLIN | EPOLLERR, int64_t timeout_ms = -1);

    /// Wait for any descriptor. If no descriptors in queue, blocks.
    /// Returns a task or notification, including `Notification::Finish` after `finish`.
    /// Lock is unlocked during waiting.
    TaskData wait(std::unique_lock<std::mutex> & lock) { return getTask(lock, -1); }

    /// Get any ready descriptor.
    /// Returns a task or notification, or an empty result if nothing is ready.
    /// Does not block.
    TaskData tryGetReadyTask(std::unique_lock<std::mutex> & lock) { return getTask(lock, 0); }

    /// Interrupt waiting permanently.
    void finish();
    /// Interrupt polling without removing an asynchronous task. Repeated notifications of the same kind coalesce.
    void notify(Notification notification);

private:
    Epoll epoll{EpollNesting::PipelinePoller};
    UnorderedMapWithMemoryTracking<Key, TaskData> tasks;

    /// In-Flight timers
    Deadlines deadlines;
    TimerDescriptor timer_signal;

    /// `Finish` stays set; polling consumes the other notification bits.
    std::atomic<uint8_t> pending_notifications = 0;
    WakeupFd notification_signal;
};
#else
class PollingQueue
{
public:
    size_t size() const { return 0; }
    bool empty() { return true; }
    void finish() {}
};
#endif

}

}
