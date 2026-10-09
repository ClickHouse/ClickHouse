#pragma once

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>

#include <optional>
#include <vector>

namespace DB
{

namespace Runtime::V2
{

class WorkerPool;

class WorkersCoordinator
{
    bool isFinished() const;
    std::optional<size_t> takeAnySleepingWorker();
    void stopLocked();

public:
    WorkersCoordinator(WorkerPool & pool_, size_t max_workers);

    void enter(size_t worker_id);
    void leave(size_t worker_id);

    bool startStealing();
    void stopStealing();
    bool hasStealer() const;

    bool startPolling();
    void stopPolling();
    bool hasPoller() const;

    bool startParking(size_t worker_id);
    void becomeNormal(size_t worker_id);
    bool wakeOne();
    bool hasSleepers() const;

    void stop();
    bool isStopped() const;

private:
    WorkerPool & pool;

    mutable Wyrd::Mutex mutex;
    std::vector<bool> is_registered;
    std::vector<size_t> sleeping_workers;

    Wyrd::Atomic<Int32> registered_workers = 0;
    Wyrd::Atomic<Int32> sleepers_count = 0;
    Wyrd::Atomic<Int32> stealing_count = 0;
    Wyrd::Atomic<bool> is_polling = false;
    Wyrd::Atomic<bool> is_stopped = false;
};

}

}
