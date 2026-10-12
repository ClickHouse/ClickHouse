#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Metrics.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingProcessor.h>
#include <Processors/Executors/Runtime/V2/Pipeline/PollRequest.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>

#include <Common/Epoll.h>
#include <Common/TimerDescriptor.h>
#include <Common/WakeupFd.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <array>
#include <memory>
#include <span>

namespace DB
{

namespace Runtime::V2
{

class WorkerPool;

#if defined(OS_LINUX) || defined(OS_DARWIN)

class Poller
{
    class Deadlines;

    bool add(const PollRequest & request);
    void unregister(ExecutingProcessor * processor);
    void updateTimer();
    size_t collectExpired(std::span<ExecutingProcessor *> out);
    size_t poll(std::span<ExecutingProcessor *> out);

public:
    explicit Poller(WorkerPool & pool_);
    ~Poller();

    void pushMany(std::span<const PollRequest> requests);
    size_t pending() const;

    void run();
    void stop();
    const PollerMetrics & getMetrics() const;

private:
    WorkerPool & pool;

    /// Registered descriptors. The mutex is not held while waiting in epoll.
    mutable Wyrd::Mutex mutex;
    Epoll epoll;
    UnorderedMapWithMemoryTracking<ExecutingProcessor *, int> fds;
    Wyrd::Atomic<size_t> pending_count = 0;

    /// System signals.
    std::unique_ptr<Deadlines> deadlines;
    TimerDescriptor timer_signal;
    WakeupFd wakeup_signal;

    /// Temporary buffers for worker.
    std::array<ExecutingProcessor *, 16> fired_buffer{};
    PollerMetrics metrics;
};

#else

class Poller
{
public:
    explicit Poller(WorkerPool &) {}

    void pushMany(std::span<const PollRequest> requests);
    size_t pending() const { return 0; }

    void run() {}
    void stop() {}
    const PollerMetrics & getMetrics() const { return metrics; }

private:
    PollerMetrics metrics;
};

#endif

}

}
