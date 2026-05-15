#include <Core/BackgroundSchedulePool.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <gtest/gtest.h>
#include <condition_variable>
#include <mutex>

using namespace DB;

TEST(BackgroundSchedulePool, Schedule)
{
    auto pool = BackgroundSchedulePool::create(4, 4, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::atomic<size_t> counter;
    std::mutex mutex;
    std::condition_variable condvar;

    static const size_t ITERATIONS = 10;
    BackgroundSchedulePoolTaskHolder task;
    task = pool->createTask(StorageID::createEmpty(), "test", [&]
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
    auto pool = BackgroundSchedulePool::create(4, 4, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::atomic<size_t> counter;
    std::mutex mutex;
    std::condition_variable condvar;

    static const size_t ITERATIONS = 10;
    BackgroundSchedulePoolTaskHolder task;
    task = pool->createTask(StorageID::createEmpty(), "test", [&]
    {
        ++counter;
        if (counter < ITERATIONS)
            ASSERT_EQ(task->scheduleAfter(1), true);
        else
            condvar.notify_one();
    });
    ASSERT_EQ(task->activateAndSchedule(), true);

    {
        std::unique_lock lock(mutex);
        condvar.wait(lock, [&] { return counter == ITERATIONS; });
    }

    ASSERT_EQ(counter, ITERATIONS);

    pool->join();
}

/// Previously leads to UB
TEST(BackgroundSchedulePool, ActivateAfterTerminitePool)
{
    BackgroundSchedulePoolTaskHolder task;
    BackgroundSchedulePoolTaskHolder delayed_task;

    {
        auto pool = BackgroundSchedulePool::create(4, 4, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

        task = pool->createTask(StorageID::createEmpty(), "test", [&] {});
        delayed_task = pool->createTask(StorageID::createEmpty(), "delayed_test", [&] {});
        pool->join();
    }

    ASSERT_EQ(task->activateAndSchedule(), false);

    ASSERT_EQ(delayed_task->activate(), true); /// does not requires pool
    ASSERT_EQ(delayed_task->scheduleAfter(1), false);
}

/// Initial size below max — the pool should grow lazily up to max under load.
TEST(BackgroundSchedulePool, LazyGrow)
{
    constexpr size_t max_threads = 8;
    constexpr size_t initial_threads = 1;
    constexpr size_t num_tasks = max_threads;

    auto pool = BackgroundSchedulePool::create(max_threads, initial_threads, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::mutex mutex;
    std::condition_variable all_running;
    std::condition_variable release_tasks;
    size_t running = 0;
    bool release = false;

    /// Distinct lambda types ⇒ distinct task groups, so they can run concurrently.
    auto make_task = [&](auto tag) {
        return pool->createTask(StorageID::createEmpty(), "lazy_grow", [&, tag] {
            (void)tag;
            std::unique_lock lock(mutex);
            ++running;
            if (running == num_tasks)
                all_running.notify_one();
            release_tasks.wait(lock, [&] { return release; });
        });
    };

    /// Hold the holders alive in a vector. The task vector pins them so they're not destroyed mid-flight.
    std::vector<BackgroundSchedulePoolTaskHolder> tasks;
    tasks.reserve(num_tasks);
    tasks.emplace_back(make_task(std::integral_constant<int, 0>{}));
    tasks.emplace_back(make_task(std::integral_constant<int, 1>{}));
    tasks.emplace_back(make_task(std::integral_constant<int, 2>{}));
    tasks.emplace_back(make_task(std::integral_constant<int, 3>{}));
    tasks.emplace_back(make_task(std::integral_constant<int, 4>{}));
    tasks.emplace_back(make_task(std::integral_constant<int, 5>{}));
    tasks.emplace_back(make_task(std::integral_constant<int, 6>{}));
    tasks.emplace_back(make_task(std::integral_constant<int, 7>{}));

    for (auto & task : tasks)
        ASSERT_EQ(task->activateAndSchedule(), true);

    {
        std::unique_lock lock(mutex);
        /// All tasks must eventually run concurrently — proves the pool grew past INITIAL_THREADS.
        all_running.wait(lock, [&] { return running == num_tasks; });
        release = true;
        release_tasks.notify_all();
    }

    pool->join();
}

/// Previously leads to UB
TEST(BackgroundSchedulePool, ScheduleAfterTerminitePool)
{
    BackgroundSchedulePoolTaskHolder task;
    BackgroundSchedulePoolTaskHolder delayed_task;

    {
        auto pool = BackgroundSchedulePool::create(4, 4, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

        task = pool->createTask(StorageID::createEmpty(), "test", [&] {});
        delayed_task = pool->createTask(StorageID::createEmpty(), "delayed_test", [&] {});

        ASSERT_EQ(task->activate(), true);
        ASSERT_EQ(delayed_task->activate(), true);
        pool->join();
    }

    ASSERT_EQ(task->schedule(), false);
    ASSERT_EQ(delayed_task->scheduleAfter(1), false);
}
