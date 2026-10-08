#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Tasks/Task.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>

#include <optional>
#include <vector>

namespace DB
{

namespace Runtime::V2
{

class Worker;
class WorkerPool;

class WorkersCoordinator
{
    bool allIdle() const;
    Worker * takeAnySleepingWorker();
    void wakeOneLocked();
    void stopLocked();

public:
    WorkersCoordinator(WorkerPool & pool_, size_t max_workers);

    void enter(size_t worker_id);
    void leave(size_t worker_id);

    std::optional<Task> wait(Worker & worker);
    size_t wake(size_t to_wake);
    bool isPolling() const;

    void stop();
    bool stopped() const;
    size_t idle() const;

private:
    WorkerPool & pool;

    mutable Wyrd::Mutex mutex;
    std::vector<bool> is_registered;
    std::vector<Worker *> sleeping_workers;

    Wyrd::Atomic<size_t> registered_workers = 0;
    Wyrd::Atomic<size_t> idle_count = 0;
    Wyrd::Atomic<size_t> polling_count = 0;
    Wyrd::Atomic<bool> is_stopped = false;
};

}

}
