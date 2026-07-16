#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <iostream>
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

    BackgroundWorker()
        : stop_(false)
        , worker_thread_(&BackgroundWorker::worker_loop, this)
        , log(getLogger("BackgroundWorker"))
    {
    }

    ~BackgroundWorker() { stop(); }

    /// Enqueue a function to be executed
    void enqueue(BackgroundWorkerTask task);

    void stop();

    bool is_empty() const;

    size_t queue_size() const;

private:
    void worker_loop();

    std::queue<BackgroundWorkerTask> task_queue_;
    mutable std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_;
    std::thread worker_thread_;
    LoggerPtr log;
};

}
