#pragma once

#include <cstdint>
#include <mutex>
#include <condition_variable>


/** Allow to subscribe for multiple events and wait for them one by one in arbitrary order.
  */
class EventCounter
{
private:
    size_t events_happened = 0;
    size_t events_waited = 0;

    mutable std::mutex mutex;
    std::condition_variable condvar;

public:
    void notify()
    {
        {
            std::lock_guard lock(mutex);
            ++events_happened;
        }
        condvar.notify_all();
    }

    void wait()
    {
        std::unique_lock lock(mutex);
        condvar.wait(lock, [&]{ return events_happened > events_waited; });
        ++events_waited;
    }

    template <typename Duration>
    bool waitFor(Duration && duration)
    {
        std::unique_lock lock(mutex);
        if (condvar.wait(lock, std::forward<Duration>(duration), [&]{ return events_happened > events_waited; }))
        {
            ++events_waited;
            return true;
        }
        return false;
    }
};
