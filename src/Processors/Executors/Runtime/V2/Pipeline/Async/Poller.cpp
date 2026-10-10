#include <Processors/Executors/Runtime/V2/Pipeline/Async/Poller.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Invariants.h>
#include <Common/Exception.h>

#include <algorithm>
#include <chrono>
#include <map>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace Runtime::V2
{

#if defined(OS_LINUX) || defined(OS_DARWIN)

class Poller::Deadlines
{
    using Clock = std::chrono::steady_clock;
    using Queue = std::multimap<Clock::time_point, ExecutingProcessor *>;

public:
    void arm(ExecutingProcessor * processor, int64_t timeout_ms)
    {
        drop(processor);
        index[processor] = queue.emplace(Clock::now() + std::chrono::milliseconds(timeout_ms), processor);
    }

    void drop(ExecutingProcessor * processor)
    {
        if (auto it = index.find(processor); it != index.end())
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

    ExecutingProcessor * popExpired()
    {
        if (queue.empty())
            return nullptr;

        auto it = queue.begin();
        auto [deadline, processor] = *it;

        if (deadline > Clock::now())
            return nullptr;

        queue.erase(it);
        index.erase(processor);
        return processor;
    }

private:
    Queue queue;
    std::unordered_map<ExecutingProcessor *, Queue::iterator> index;
};

void Poller::unregister(ExecutingProcessor * processor)
{
    auto it = fds.find(processor);
    if (it == fds.end())
        Wyrd::abort("Processor {} is not registered in the poller", fmt::ptr(processor));

    epoll.remove(it->second);
    fds.erase(it);
    deadlines->drop(processor);
    pending_count.fetch_sub(1);
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

void Poller::collectExpired(std::vector<ExecutingProcessor *> & fired)
{
    while (ExecutingProcessor * expired = deadlines->popExpired())
    {
        unregister(expired);
        expired->status.store(ExecutingProcessor::Status::AsyncReady);
        fired.push_back(expired);
    }

    updateTimer();
}

Poller::Poller()
    : epoll(EpollNesting::PipelinePoller)
    , deadlines(std::make_unique<Deadlines>())
{
    epoll.add(wakeup_signal.fd(), &wakeup_signal);
    epoll.add(timer_signal.getDescriptor(), &timer_signal);
    Wyrd::setName(mutex, "poller.mutex");
    Wyrd::setName(pending_count, "poller.pending_count");
}

Poller::~Poller() = default;

bool Poller::add(const PollRequest & request)
{
    if (!fds.emplace(request.processor, request.fd).second)
        Wyrd::abort("Processor {} is already registered in the poller", fmt::ptr(request.processor));

    epoll.add(request.fd, request.processor, request.events);

    if (request.timeout_ms < 0)
        return false;

    deadlines->arm(request.processor, request.timeout_ms);
    return true;
}

void Poller::pushMany(std::span<const PollRequest> requests)
{
    if (requests.empty())
        return;

    std::lock_guard lock(mutex);

    bool armed = false;
    for (const auto & request : requests)
        armed |= add(request);

    pending_count.fetch_add(requests.size());

    if (armed)
        updateTimer();
}

std::vector<ExecutingProcessor *> Poller::poll(int timeout_ms)
{
    std::vector<ExecutingProcessor *> fired;

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

        auto * processor = static_cast<ExecutingProcessor *>(ptr);
        unregister(processor);
        processor->status.store(ExecutingProcessor::Status::AsyncReady);
        fired.push_back(processor);
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

void Poller::pushMany(std::span<const PollRequest> requests)
{
    if (!requests.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Async processors are not supported on this platform");
}

#endif

}

}
