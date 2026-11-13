#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>

#include <Common/logger_useful.h>

namespace BuzzHouse
{

class BackgroundWorker
{
public:
    using BackgroundWorkerTask = std::function<void()>;

    explicit BackgroundWorker(LoggerPtr log_)
        : stop_(false)
        , log(std::move(log_))
        , worker_thread_(&BackgroundWorker::worker_loop, this)
    {
    }

    ~BackgroundWorker() { stop(); }

    /// Enqueue a function to be executed
    void enqueue(BackgroundWorkerTask task);

    void stop();

private:
    void worker_loop();

    std::queue<BackgroundWorkerTask> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_;
    LoggerPtr log;
    std::thread worker_thread_;
};

}
