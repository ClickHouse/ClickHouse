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

    bool all_ran = false;
    {
        std::unique_lock lock(mutex);
        /// All tasks must eventually run concurrently — proves the pool grew past INITIAL_THREADS.
        /// Bounded wait so a lazy-growth regression fails the test instead of hanging it, and
        /// release the blocked tasks before asserting so join() never deadlocks on a failure.
        all_ran = all_running.wait_for(lock, std::chrono::seconds(60), [&] { return running == num_tasks; });
        release = true;
        release_tasks.notify_all();
    }

    ASSERT_TRUE(all_ran);

    pool->join();
}

/// Many tasks of the *same* task type must also grow the pool past the initial size: a single task
/// group can run up to max_parallel_tasks_per_type tasks concurrently, so lazy growth must be driven
/// by the number of runnable tasks, not by the number of distinct runnable task types.
TEST(BackgroundSchedulePool, LazyGrowSameTaskType)
{
    constexpr size_t max_threads = 8;
    constexpr size_t initial_threads = 1;
    constexpr size_t num_tasks = max_threads;

    /// max_parallel_tasks_per_type == 0 ⇒ defaults to the pool size, so a single group may run all tasks.
    auto pool = BackgroundSchedulePool::create(max_threads, initial_threads, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::mutex mutex;
    std::condition_variable all_running;
    std::condition_variable release_tasks;
    size_t running = 0;
    bool release = false;

    /// A single lambda expression ⇒ one closure type ⇒ all tasks share one task group.
    auto task_body = [&]
    {
        std::unique_lock lock(mutex);
        ++running;
        if (running == num_tasks)
            all_running.notify_one();
        release_tasks.wait(lock, [&] { return release; });
    };

    std::vector<BackgroundSchedulePoolTaskHolder> tasks;
    tasks.reserve(num_tasks);
    for (size_t i = 0; i < num_tasks; ++i)
        tasks.emplace_back(pool->createTask(StorageID::createEmpty(), "lazy_grow_same_type", task_body));

    for (auto & task : tasks)
        ASSERT_EQ(task->activateAndSchedule(), true);

    bool all_ran = false;
    {
        std::unique_lock lock(mutex);
        /// All same-type tasks must run concurrently — proves the pool grew past INITIAL_THREADS.
        /// Bounded wait so a regression fails the test instead of hanging it.
        all_ran = all_running.wait_for(lock, std::chrono::seconds(60), [&] { return running == num_tasks; });
        release = true;
        release_tasks.notify_all();
    }

    ASSERT_TRUE(all_ran);

    pool->join();
}

/// A pool created with zero initial workers (background_schedule_pool_initial_size = 0) must spawn
/// its very first worker on demand and actually run scheduled work, instead of accepting the schedule
/// and leaving the task stuck. This is the riskiest cold-start boundary: the production code has
/// dedicated first-worker handling (the threads.empty() rollback/retry path in scheduleTask), and the
/// other tests only exercise pools that start with at least one worker.
TEST(BackgroundSchedulePool, ZeroInitialSchedule)
{
    auto pool = BackgroundSchedulePool::create(1, 0, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::mutex mutex;
    std::condition_variable condvar;
    bool ran = false;

    BackgroundSchedulePoolTaskHolder task;
    task = pool->createTask(StorageID::createEmpty(), "zero_initial", [&]
    {
        std::lock_guard lock(mutex);
        ran = true;
        condvar.notify_one();
    });
    ASSERT_EQ(task->activateAndSchedule(), true);

    bool finished = false;
    {
        std::unique_lock lock(mutex);
        /// Bounded wait so a cold-start regression fails the test instead of hanging it.
        finished = condvar.wait_for(lock, std::chrono::seconds(60), [&] { return ran; });
    }

    ASSERT_TRUE(finished);

    pool->join();
}

/// Same zero-initial cold-start boundary, but reached through scheduleAfter: the first scheduleTask is
/// driven by the delayed-execution thread, which now catches and retries a first-worker spawn failure
/// instead of letting the exception terminate the delayed thread (which would stall all future
/// scheduleAfter tasks in the pool). Here the spawn succeeds, so the delayed task must run.
TEST(BackgroundSchedulePool, ZeroInitialScheduleAfter)
{
    auto pool = BackgroundSchedulePool::create(1, 0, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::mutex mutex;
    std::condition_variable condvar;
    bool ran = false;

    BackgroundSchedulePoolTaskHolder task;
    task = pool->createTask(StorageID::createEmpty(), "zero_initial_delayed", [&]
    {
        std::lock_guard lock(mutex);
        ran = true;
        condvar.notify_one();
    });
    ASSERT_EQ(task->activate(), true);
    ASSERT_EQ(task->scheduleAfter(1), true);

    bool finished = false;
    {
        std::unique_lock lock(mutex);
        /// Bounded wait so a cold-start regression fails the test instead of hanging it.
        finished = condvar.wait_for(lock, std::chrono::seconds(60), [&] { return ran; });
    }

    ASSERT_TRUE(finished);

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
