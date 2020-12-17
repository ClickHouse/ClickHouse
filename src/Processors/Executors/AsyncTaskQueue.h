#pragma once
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <atomic>
#include <unordered_map>

namespace DB
{

#if defined(OS_LINUX)
class AsyncTaskQueue
{
public:
    struct TaskData
    {
        size_t thread_num;

        void * data = nullptr;
        int fd = -1;


        explicit operator bool() const { return data; }
    };

private:
    int epoll_fd;
    int pipe_fd[2];
    std::atomic_bool is_finished = false;
    std::unordered_map<std::uintptr_t, TaskData> tasks;

public:
    AsyncTaskQueue();
    ~AsyncTaskQueue();

    size_t size() const { return tasks.size(); }
    bool empty() const { return tasks.empty(); }

    /// Add new task to queue.
    void addTask(size_t thread_number, void * data, int fd);

    /// Wait for any descriptor. If no descriptors in queue, blocks.
    /// Returns ptr which was inserted into queue or nullptr if finished was called.
    /// Lock is used to wait on condvar.
    TaskData wait(std::unique_lock<std::mutex> & lock);

    /// Interrupt waiting.
    void finish();
};
#else
class AsyncTaskQueue
{
public:
    bool empty() { return true; }
    void finish() {}
};
#endif

}
