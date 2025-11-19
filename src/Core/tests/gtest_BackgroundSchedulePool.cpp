#include <Core/BackgroundSchedulePool.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <gtest/gtest.h>
#include <condition_variable>
#include <mutex>

using namespace DB;

TEST(BackgroundSchedulePool, Schedule)
{
    auto pool = BackgroundSchedulePool::create(4, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::atomic<size_t> counter;
    std::mutex mutex;
    std::condition_variable condvar;

    static const size_t ITERATIONS = 10;
    BackgroundSchedulePoolTaskHolder task;
    task = pool->createTask("test", [&]
    {
        ++counter;
        if (counter < ITERATIONS)
            ASSERT_EQ(task->schedule(), true);
        else
            condvar.notify_one();
    });
    ASSERT_EQ(task->activateAndSchedule(), true);

    std::unique_lock lock(mutex);
    condvar.wait(lock, [&] { return counter == ITERATIONS; });

    ASSERT_EQ(counter, ITERATIONS);

    pool->join();
}

TEST(BackgroundSchedulePool, ScheduleAfter)
{
    auto pool = BackgroundSchedulePool::create(4, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::atomic<size_t> counter;
    std::mutex mutex;
    std::condition_variable condvar;

    static const size_t ITERATIONS = 10;
    BackgroundSchedulePoolTaskHolder task;
    task = pool->createTask("test", [&]
    {
        ++counter;
        if (counter < ITERATIONS)
            ASSERT_EQ(task->scheduleAfter(1), true);
        else
            condvar.notify_one();
    });
    ASSERT_EQ(task->activateAndSchedule(), true);

    std::unique_lock lock(mutex);
    condvar.wait(lock, [&] { return counter == ITERATIONS; });

    ASSERT_EQ(counter, ITERATIONS);

    pool->join();
}

/// Previously leads to UB
TEST(BackgroundSchedulePool, ActivateAfterTerminitePool)
{
    BackgroundSchedulePoolTaskHolder task;
    BackgroundSchedulePoolTaskHolder delayed_task;

    {
        auto pool = BackgroundSchedulePool::create(4, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

        task = pool->createTask("test", [&] {});
        delayed_task = pool->createTask("delayed_test", [&] {});
        pool->join();
    }

    ASSERT_EQ(task->activateAndSchedule(), false);

    ASSERT_EQ(delayed_task->activate(), true); /// does not requires pool
    ASSERT_EQ(delayed_task->scheduleAfter(1), false);
}

/// Previously leads to UB
TEST(BackgroundSchedulePool, ScheduleAfterTerminitePool)
{
    BackgroundSchedulePoolTaskHolder task;
    BackgroundSchedulePoolTaskHolder delayed_task;

    {
        auto pool = BackgroundSchedulePool::create(4, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

        task = pool->createTask("test", [&] {});
        delayed_task = pool->createTask("delayed_test", [&] {});

        ASSERT_EQ(task->activate(), true);
        ASSERT_EQ(delayed_task->activate(), true);
        pool->join();
    }

    ASSERT_EQ(task->schedule(), false);
    ASSERT_EQ(delayed_task->scheduleAfter(1), false);
}
