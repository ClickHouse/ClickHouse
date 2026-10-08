#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Polling/PollRequest.h>
#include <Processors/Executors/Runtime/V2/Engine/Tasks/Task.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>

#include <Common/Epoll.h>
#include <Common/TimerDescriptor.h>
#include <Common/WakeupFd.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace DB
{


namespace Runtime::V2
{

#if defined(OS_LINUX) || defined(OS_DARWIN)

class Poller
{
    class Deadlines;

    void unregister(ProcessorState * state);
    void updateTimer();
    void collectExpired(std::vector<Task> & fired);

public:
    Poller();
    ~Poller();

    void add(const PollRequest & request);
    std::vector<Task> poll(int timeout_ms);
    size_t pending() const;
    void wakeup();

private:
    /// Registered descriptors. The mutex is not held while waiting in epoll.
    mutable Wyrd::Mutex mutex;
    Epoll epoll;
    std::unordered_map<ProcessorState *, int> fds;
    Wyrd::Atomic<size_t> pending_count = 0;

    /// In-flight timers
    std::unique_ptr<Deadlines> deadlines;
    TimerDescriptor timer_signal;

    /// One waiter at a time; a second poll returns nothing instead of waiting
    Wyrd::Atomic<bool> polling = false;
    WakeupFd wakeup_signal;
};

#else

class Poller
{
public:
    void add(const PollRequest & request);
    std::vector<Task> poll(int) { return {}; }
    size_t pending() const { return 0; }
    void wakeup() {}
};

#endif

}

}
