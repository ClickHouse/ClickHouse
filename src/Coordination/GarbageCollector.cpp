#include "Coordination/GarbageCollector.h"

namespace DB
{

GarbageCollector::GarbageCollector(bool enable_background_thread, size_t limit_)
    : stop_flag(false)
    , limit(limit_)
{
    if (enable_background_thread)
        worker_thread = std::thread(&GarbageCollector::processQueue, this);
}

GarbageCollector::~GarbageCollector()
{
    stop_flag.store(true);
    worker_thread.join();
}


void GarbageCollector::addNode(const std::string & node, std::chrono::milliseconds delay, std::function<void()> callback)
{
    TimePoint execute_at = Clock::now() + delay;
    {
        std::unique_lock lock(mutex);

        if (nodes_to_process.size() > limit)
            return;
        nodes_to_process[node]++;
        event_queue.emplace(Event{execute_at, std::move(callback), node});
    }
    cv.notify_all();
}

void GarbageCollector::processQueue()
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

                --nodes_to_process[event.node];
                bool should_process_delete = nodes_to_process[event.node] == 0;
                lock.unlock();

                if (should_process_delete)
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
