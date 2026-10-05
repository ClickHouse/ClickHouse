#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Poller.h>
#include <Processors/Executors/Runtime/V2/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>

#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

namespace Runtime::V2
{

class WorkersCoordinator
{
    struct alignas(128) ParkingSpot
    {
        Wyrd::Atomic<bool> wake_up = false;

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
    size_t wake(size_t to_wake);
    bool needsPoller() const;

    void stop();
    bool stopped() const;
    size_t idle() const;
    size_t registered() const;

private:
    TaskScheduler & scheduler;
    Poller & poller;

    mutable Wyrd::Mutex mutex;

    /// What threads are registered in coordinator.
    std::vector<bool> is_registered;

    /// Threads sleeping coordination
    std::vector<ParkingSpot> sleeping_spots;
    std::vector<size_t> sleeping_threads;

    /// Statistics
    Wyrd::Atomic<size_t> registered_workers = 0;
    Wyrd::Atomic<size_t> idle_count = 0;
    Wyrd::Atomic<size_t> sleeping_count = 0;
    Wyrd::Atomic<size_t> polling_count = 0;
    Wyrd::Atomic<bool> is_stopped = false;
};

}

}
