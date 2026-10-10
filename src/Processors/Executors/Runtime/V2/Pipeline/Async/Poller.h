#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/Async/PollRequest.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingProcessor.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>

#include <Common/Epoll.h>
#include <Common/TimerDescriptor.h>
#include <Common/WakeupFd.h>

#include <memory>
#include <mutex>
#include <span>
#include <unordered_map>
#include <vector>

namespace DB
{


namespace Runtime::V2
{

#if defined(OS_LINUX) || defined(OS_DARWIN)

/// TODO: Support Clock + TimeTable in Wyrd
class Poller
{
    class Deadlines;

    bool add(const PollRequest & request);
    void unregister(ExecutingProcessor * processor);
    void updateTimer();
    void collectExpired(std::vector<ExecutingProcessor *> & fired);

public:
    Poller();
    ~Poller();

    void pushMany(std::span<const PollRequest> requests);
    std::vector<ExecutingProcessor *> poll(int timeout_ms);
    size_t pending() const;
    void wakeup();

private:
    /// Registered descriptors. The mutex is not held while waiting in epoll.
    mutable Wyrd::Mutex mutex;
    Epoll epoll;
    std::unordered_map<ExecutingProcessor *, int> fds;
    Wyrd::Atomic<size_t> pending_count = 0;

    /// In-flight timers
    std::unique_ptr<Deadlines> deadlines;
    TimerDescriptor timer_signal;

    WakeupFd wakeup_signal;
};

#else

class Poller
{
public:
    void pushMany(std::span<const PollRequest> requests);
    std::vector<ExecutingProcessor *> poll(int) { return {}; }
    size_t pending() const { return 0; }
    void wakeup() {}
};

#endif

}

}
