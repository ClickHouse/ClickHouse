#pragma once

#include <Common/ThreadPool.h>

#include <functional>
#include <thread>


/// Like `std::thread`, but the thread holds a `ThreadStatus` and the global profiler periods, as a thread of
/// `ThreadFromGlobalPool` does, without taking a thread of `GlobalThreadPool`.
class ThreadWithThreadStatus
{
public:
    ThreadWithThreadStatus() = default;

    template <typename Function, typename... Args>
    explicit ThreadWithThreadStatus(Function && func, Args &&... args)
        : thread(
              &ThreadWithThreadStatus::run,
              std::function<void()>(std::bind_front(std::forward<Function>(func), std::forward<Args>(args)...)),
              GlobalThreadPool::instance().global_profiler_real_time_period_ns,
              GlobalThreadPool::instance().global_profiler_cpu_time_period_ns)
    {
    }

    ThreadWithThreadStatus(ThreadWithThreadStatus &&) noexcept = default;
    ThreadWithThreadStatus & operator=(ThreadWithThreadStatus &&) noexcept = default;

    [[nodiscard]] bool joinable() const noexcept { return thread.joinable(); }
    void join() { thread.join(); }
    void detach() { thread.detach(); }

private:
    static void run(std::function<void()> func, UInt64 real_time_period_ns, UInt64 cpu_time_period_ns);

    GlobalThreadType thread;
};
