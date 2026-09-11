#pragma once

#include <Common/Epoll.h>
#include <Common/TimerDescriptor.h>
#include <Common/WakeupFd.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace DB
{

struct ProcessorState;

#if defined(OS_LINUX) || defined(OS_DARWIN)

class Poller
{
    class Deadlines;

    void unregister(ProcessorState * state);
    void updateTimer();
    void collectExpired(std::vector<ProcessorState *> & fired);

public:
    Poller();
    ~Poller();

    void add(ProcessorState & state, int fd, uint32_t events = EPOLLIN | EPOLLERR, int64_t timeout_ms = -1);
    std::vector<ProcessorState *> poll(int timeout_ms);
    size_t pending() const;
    void wakeup();

private:
    /// Registered descriptors. The mutex is not held while waiting in epoll.
    mutable std::mutex mutex;
    Epoll epoll;
    std::unordered_map<ProcessorState *, int> fds;
    std::atomic<size_t> pending_count = 0;

    /// In-flight timers
    std::unique_ptr<Deadlines> deadlines;
    TimerDescriptor timer_signal;

    /// One waiter at a time; a second poll returns nothing instead of waiting
    std::atomic_bool polling = false;
    WakeupFd wakeup_signal;
};

#else

class Poller
{
public:
    void add(ProcessorState & state, int fd, uint32_t events = 0, int64_t timeout_ms = -1);
    std::vector<ProcessorState *> poll(int) { return {}; }
    size_t pending() const { return 0; }
    void wakeup() {}
};

#endif

}
