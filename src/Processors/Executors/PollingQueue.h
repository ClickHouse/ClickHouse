#pragma once
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <atomic>
#include <unordered_map>
#include <Common/Epoll.h>

namespace DB
{

#if defined(OS_LINUX)

/// This queue is used to poll descriptors. Generally, just a wrapper over epoll.
class PollingQueue
{
public:
    struct TaskData
    {
        size_t thread_num = 0;

        void * data = nullptr;
        int fd = -1;

        explicit operator bool() const { return data; }
    };

private:
    Epoll epoll;
    int pipe_fd[2];
    std::atomic_bool is_finished = false;
    std::unordered_map<std::uintptr_t, TaskData> tasks;

    TaskData getTask(std::unique_lock<std::mutex> & lock, int timeout);

public:
    PollingQueue();
    ~PollingQueue();

    size_t size() const { return tasks.size(); }
    bool empty() const { return tasks.empty(); }

    /// Add new task to queue.
    void addTask(size_t thread_number, void * data, int fd);

    /// Wait for any descriptor. If no descriptors in queue, blocks.
    /// Returns ptr which was inserted into queue or nullptr if finished was called.
    /// Lock is unlocked during waiting.
    TaskData wait(std::unique_lock<std::mutex> & lock) { return getTask(lock, -1); }

    /// Get any ready descriptor.
    /// Returns nullptr if no descriptor is ready or if finished was called.
    /// Does not block.
    TaskData tryGetReadyTask(std::unique_lock<std::mutex> & lock) { return getTask(lock, 0); }

    /// Interrupt waiting.
    void finish();
};
#else
class PollingQueue
{
public:
    bool empty() { return true; }
    void finish() {}
};
#endif

}
