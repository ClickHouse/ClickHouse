#pragma once

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

#include <optional>

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
    size_t registeredWorkers() const;

    bool startStealing();
    void stopStealing();
    bool hasStealer() const;

    bool startParking(size_t worker_id);
    void becomeNormal(size_t worker_id);
    bool wakeOne();
    bool hasSleepers() const;

    void stop();
    bool isStopped() const;

private:
    WorkerPool & pool;

    mutable Wyrd::Mutex mutex;
    VectorWithMemoryTracking<bool> is_registered;
    VectorWithMemoryTracking<size_t> sleeping_workers;

    Wyrd::Atomic<Int32> registered_workers = 0;
    Wyrd::Atomic<Int32> sleepers_count = 0;
    Wyrd::Atomic<Int32> stealing_count = 0;
    Wyrd::Atomic<bool> is_stopped = false;
};

}

}
