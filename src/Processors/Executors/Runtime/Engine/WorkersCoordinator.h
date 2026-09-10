#pragma once

#include <Processors/Executors/Runtime/Engine/Poller.h>
#include <Processors/Executors/Runtime/Engine/TaskScheduler.h>

#include <atomic>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

class WorkersCoordinator
{
    struct alignas(128) ParkingSpot
    {
        std::atomic<bool> wake_up = false;

        void park();
        void unpark();
    };

    bool allIdle(size_t idle_workers) const;
    std::optional<size_t> takeAnySleepingThread();
    void wakeOneLocked();
    void stopLocked();

public:
    WorkersCoordinator(TaskScheduler & scheduler_, Poller & poller_, size_t max_workers);

    void enter(size_t worker_id);
    void leave(size_t worker_id);

    bool wait(size_t worker_id);
    void wake(size_t to_wake);
    bool needsPoller() const;

    void stop();
    bool stopped() const;
    size_t idle() const;
    size_t registered() const;

private:
    TaskScheduler & scheduler;
    Poller & poller;

    mutable std::mutex mutex;

    /// What threads are registered in coordinator.
    std::vector<bool> is_registered;

    /// Threads sleeping coordination
    std::vector<ParkingSpot> sleeping_spots;
    std::vector<size_t> sleeping_threads;

    /// Statistics
    std::atomic<size_t> registered_workers = 0;
    std::atomic<size_t> idle_count = 0;
    std::atomic<size_t> sleeping_count = 0;
    std::atomic<size_t> polling_count = 0;
    std::atomic_bool is_stopped = false;
};

}
