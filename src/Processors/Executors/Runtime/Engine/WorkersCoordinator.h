#pragma once

#include <Processors/Executors/Runtime/Engine/Poller.h>
#include <Processors/Executors/Runtime/Engine/TaskScheduler.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <vector>

namespace DB
{

class WorkersCoordinator
{
    bool allIdle(size_t idle_workers) const;
    void wakeOneLocked();
    void stopLocked();

public:
    WorkersCoordinator(TaskScheduler & scheduler_, Poller & poller_, size_t max_workers);

    void enter(size_t worker_id);
    void leave(size_t worker_id);

    bool wait(size_t worker_id);
    void wakeOne();
    bool needsPoller() const;

    void stop();
    bool stopped() const;
    size_t idle() const;
    size_t registered() const;

private:
    TaskScheduler & scheduler;
    Poller & poller;

    mutable std::mutex mutex;
    std::condition_variable have_work;
    std::vector<bool> is_registered;
    std::atomic<size_t> registered_workers = 0;
    std::atomic<size_t> idle_count = 0;
    std::atomic<size_t> sleeping_count = 0;
    std::atomic<size_t> polling_count = 0;
    std::atomic_bool is_stopped = false;
};

}
