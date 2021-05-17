#include <chrono>
#include <thread>

#include <Common/Coverage.h>

#include <gtest/gtest.h>

using detail::TaskQueue;

TEST(CoverageRuntime, TaskQueueSimple)
{
    auto queue = TaskQueue{5};

    auto f = [] (size_t)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));
    };

    for ([[maybe_unused]] auto i : {0,1,2,3,4})
        queue.schedule(f);

    queue.waitAndRespawnThreads(3);

    for ([[maybe_unused]] auto i : {0,1,2})
        queue.schedule(f);

    queue.waitAndRespawnThreads(0);
}

TEST(CoverageRuntime, TaskQueueWithWaiting)
{
    auto queue = TaskQueue{5};

    auto f = [] (size_t)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));
    };

    for ([[maybe_unused]] auto i : {0,1,2,3,4,5,6,7,8,9})
        queue.schedule(f);

    queue.waitAndRespawnThreads(0);
}
