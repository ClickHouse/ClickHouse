#include <Processors/Executors/Runtime/Engine/Poller.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <chrono>
#include <map>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

#if defined(OS_LINUX) || defined(OS_DARWIN)

class Poller::Deadlines
{
    using Clock = std::chrono::steady_clock;
    using Queue = std::multimap<Clock::time_point, ProcessorState *>;

public:
    void arm(ProcessorState * state, int64_t timeout_ms)
    {
        cancel(state);
        index[state] = queue.emplace(Clock::now() + std::chrono::milliseconds(timeout_ms), state);
    }

    void cancel(ProcessorState * state)
    {
        if (auto it = index.find(state); it != index.end())
        {
            queue.erase(it->second);
            index.erase(it);
        }
    }

    std::optional<Clock::time_point> next() const
    {
        if (queue.empty())
            return std::nullopt;

        return queue.begin()->first;
    }

    ProcessorState * popExpired()
    {
        if (queue.empty())
            return nullptr;

        auto it = queue.begin();
        auto [deadline, state] = *it;

        if (deadline > Clock::now())
            return nullptr;

        queue.erase(it);
        index.erase(state);
        return state;
    }

private:
    Queue queue;
    std::unordered_map<ProcessorState *, Queue::iterator> index;
};

void Poller::unregister(ProcessorState * state)
{
    auto it = fds.find(state);
    if (it == fds.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor state {} is not registered in the poller", static_cast<const void *>(state));

    epoll.remove(it->second);
    fds.erase(it);
    --pending_count;
    deadlines->cancel(state);
}

void Poller::updateTimer()
{
    auto next = deadlines->next();
    if (!next)
    {
        timer_signal.reset();
        return;
    }

    auto us_until_deadline = std::chrono::duration_cast<std::chrono::microseconds>(*next - std::chrono::steady_clock::now()).count();
    timer_signal.setRelative(std::max<int64_t>(1, us_until_deadline));
}

void Poller::collectExpired(std::vector<ProcessorState *> & fired)
{
    while (ProcessorState * expired = deadlines->popExpired())
    {
        unregister(expired);
        fired.push_back(expired);
    }

    updateTimer();
}

Poller::Poller()
    : deadlines(std::make_unique<Deadlines>())
{
    epoll.add(wakeup_signal.fd(), &wakeup_signal);
    epoll.add(timer_signal.getDescriptor(), &timer_signal);
}

Poller::~Poller() = default;

void Poller::add(ProcessorState & state, int fd, uint32_t events, int64_t timeout_ms)
{
    std::lock_guard lock(mutex);

    if (!fds.emplace(&state, fd).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor state {} is already registered in the poller", static_cast<const void *>(&state));

    epoll.add(fd, &state, events);
    ++pending_count;

    if (timeout_ms >= 0)
    {
        deadlines->arm(&state, timeout_ms);
        updateTimer();
    }
}

std::vector<ProcessorState *> Poller::poll(int timeout_ms)
{
    if (polling.exchange(true))
        return {};

    SCOPE_EXIT(polling = false);

    std::vector<ProcessorState *> fired;

    {
        std::lock_guard lock(mutex);
        collectExpired(fired);
        if (!fired.empty())
            return fired;
    }

    constexpr size_t max_events = 16;
    epoll_event events[max_events]{};
    size_t num_events = epoll.getManyReady(max_events, events, timeout_ms);

    std::lock_guard lock(mutex);

    bool timer_fired = false;
    for (size_t i = 0; i < num_events; ++i)
    {
        void * ptr = events[i].data.ptr;

        if (ptr == &wakeup_signal)
        {
            wakeup_signal.drain();
            continue;
        }

        if (ptr == &timer_signal)
        {
            timer_signal.drain();
            timer_fired = true;
            continue;
        }

        auto * state = static_cast<ProcessorState *>(ptr);
        unregister(state);
        fired.push_back(state);
    }

    if (timer_fired)
        collectExpired(fired);

    return fired;
}

size_t Poller::pending() const
{
    return pending_count.load();
}

void Poller::wakeup()
{
    wakeup_signal.notify();
}

#else

void Poller::add(ProcessorState &, int, uint32_t, int64_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Async processors are not supported on this platform");
}

#endif

}
