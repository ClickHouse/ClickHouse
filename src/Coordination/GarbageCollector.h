#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_set>

namespace DB
{

/// The garbage collector is used to remove nodes with TTL.
/// It does not clean up nodes from server recovery - they are removed lazily
class GarbageCollector
{
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Callback = std::function<void()>;

    explicit GarbageCollector();
    ~GarbageCollector();

    void addNode(const std::string & node, std::chrono::milliseconds delay, std::function<void()> callback);

private:
    void processQueue();

    struct Event
    {
        TimePoint time;
        std::function<void()> func;
        std::string node;

        bool operator>(const Event & other) const { return time > other.time; }
    };

    std::priority_queue<Event, std::vector<Event>, std::greater<>> event_queue;
    std::unordered_set<std::string> nodes_to_process;
    std::mutex mutex;
    std::condition_variable cv;
    std::thread worker_thread;
    std::atomic<bool> stop_flag;
};

}
