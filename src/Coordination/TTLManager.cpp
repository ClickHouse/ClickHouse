#include "Coordination/TTLManager.h"

#include <iostream>

namespace DB
{

TTLManager::TTLManager()
    : worker_thread(&TTLManager::processQueue, this)
    , stop_flag(false)
{
}

TTLManager::~TTLManager()
{
    stop_flag.store(true);
    worker_thread.join();
}


void TTLManager::addNode(std::chrono::milliseconds delay, std::function<void()> callback)
{
    TimePoint execute_at = Clock::now() + delay;
    {
        std::cerr << "add node " << event_queue.size() << '\n';
        std::unique_lock lock(mutex);
        event_queue.emplace(Event{execute_at, std::move(callback)});
    }
    cv.notify_all();

}

void TTLManager::processQueue()
{
    while (!stop_flag) 
    {
        std::unique_lock lock(mutex);

        if (event_queue.empty())
        {
            cv.wait(lock, [&] { return stop_flag || !event_queue.empty(); });
        }
        else
        {
            auto now = Clock::now();
            auto next_time = event_queue.top().time;

            if (now >= next_time) 
            {
                auto event = event_queue.top();
                event_queue.pop();
                lock.unlock();
                std::cerr << "execute callback " << event_queue.size() << '\n';
                event.func();
            }
            else
            {
                cv.wait_until(lock, next_time);
            }
        }
    }

}

}
