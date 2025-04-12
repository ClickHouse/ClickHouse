#pragma once

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <chrono>
#include <atomic>

namespace DB
{

class TTLManager
{
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Callback = std::function<void()>;

    explicit TTLManager();
    ~TTLManager();

    void addNode(std::chrono::milliseconds delay, std::function<void()> callback);

private:
    void processQueue();

    struct Event 
    {
        TimePoint time;
        std::function<void()> func;

        bool operator>(const Event& other) const 
        {
            return time > other.time;
        }
    };

    std::priority_queue<Event, std::vector<Event>, std::greater<>> event_queue;
    std::mutex mutex;
    std::condition_variable cv;
    std::thread worker_thread;
    std::atomic<bool> stop_flag;
};

}
