#include <Core/BackgroundSchedulePool.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <base/scope_guard.h>
#include <gtest/gtest.h>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <thread>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int CANNOT_SCHEDULE_TASK;
}

TEST(BackgroundSchedulePool, Schedule)
{
    auto pool = BackgroundSchedulePool::create(4, 4, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::atomic<size_t> counter{0};
    std::mutex mutex;
    std::condition_variable condvar;

    static const size_t ITERATIONS = 10;
    BackgroundSchedulePoolTaskHolder task;
    task = pool->createTask(StorageID::createEmpty(), "test", [&]
    {
        /// counter is published under the waiter's mutex, so the increment that completes it is
        /// visible to a waiter that has already evaluated its predicate.
        std::lock_guard lock(mutex);
        ++counter;
        if (counter < ITERATIONS)
            ASSERT_EQ(task->schedule(), true);
        else
            condvar.notify_one();
    });
    ASSERT_EQ(task->activateAndSchedule(), true);

    bool timed_out = false;
    {
        std::unique_lock lock(mutex);
        /// The wait status decides, not the predicate: a dropped notification leaves the predicate
        /// true, so the predicate overload of wait_for would report success after the deadline.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (counter != ITERATIONS && !timed_out)
            timed_out = condvar.wait_until(lock, deadline) == std::cv_status::timeout;
    }

    /// The pool's destructor requires join(), so a fatal assertion must not precede it.
    pool->join();

    EXPECT_FALSE(timed_out) << "timed out with " << counter << " of " << ITERATIONS << " iterations run";
    ASSERT_EQ(counter, ITERATIONS);
}

TEST(BackgroundSchedulePool, ScheduleAfter)
{
    auto pool = BackgroundSchedulePool::create(4, 4, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::atomic<size_t> counter{0};
    std::mutex mutex;
    std::condition_variable condvar;

    static const size_t ITERATIONS = 10;
    BackgroundSchedulePoolTaskHolder task;
    task = pool->createTask(StorageID::createEmpty(), "test", [&]
    {
        /// counter is published under the waiter's mutex, so the increment that completes it is
        /// visible to a waiter that has already evaluated its predicate.
        std::lock_guard lock(mutex);
        ++counter;
        if (counter < ITERATIONS)
            ASSERT_EQ(task->scheduleAfter(1), true);
        else
            condvar.notify_one();
    });
    ASSERT_EQ(task->activateAndSchedule(), true);

    bool timed_out = false;
    {
        std::unique_lock lock(mutex);
        /// The wait status decides, not the predicate: a dropped notification leaves the predicate
        /// true, so the predicate overload of wait_for would report success after the deadline.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (counter != ITERATIONS && !timed_out)
            timed_out = condvar.wait_until(lock, deadline) == std::cv_status::timeout;
    }

    /// The pool's destructor requires join(), so a fatal assertion must not precede it.
    pool->join();

    EXPECT_FALSE(timed_out) << "timed out with " << counter << " of " << ITERATIONS << " iterations run";
    ASSERT_EQ(counter, ITERATIONS);
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

/// Failing to spawn a schedule pool's initial threads must throw a recoverable exception, not
/// abort the server. The fault injector forces the spawn to throw.
TEST(BackgroundSchedulePool, CtorThrowsInsteadOfAbortOnSpawnFailure)
{
    CannotAllocateThreadFaultInjector::setFaultProbability(1.0);
    SCOPE_EXIT({ CannotAllocateThreadFaultInjector::setFaultProbability(0.0); });

    bool threw_cannot_schedule = false;
    try
    {
        auto pool = BackgroundSchedulePool::create(4, 4, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);
    }
    catch (const Exception & e)
    {
        threw_cannot_schedule = (e.code() == ErrorCodes::CANNOT_SCHEDULE_TASK);
    }

    EXPECT_TRUE(threw_cannot_schedule) << "constructor must throw CANNOT_SCHEDULE_TASK, not abort, when it cannot spawn initial threads";
}

/// The case above fails the first spawn, so `threads` is empty and the teardown has nothing to
/// join. A partial fault probability makes some constructions fail after a worker already
/// started, which is what exercises the join. Without it this test aborts rather than fails.
TEST(BackgroundSchedulePool, CtorJoinsPartiallySpawnedThreadsOnFailure)
{
    CannotAllocateThreadFaultInjector::setFaultProbability(0.5);
    SCOPE_EXIT({ CannotAllocateThreadFaultInjector::setFaultProbability(0.0); });

    size_t threw = 0;
    size_t constructed = 0;
    for (size_t i = 0; i < 200; ++i)
    {
        try
        {
            auto pool
                = BackgroundSchedulePool::create(8, 8, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);
            ++constructed;
            pool->join();
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::CANNOT_SCHEDULE_TASK);
            ++threw;
        }
    }

    /// With 8 initial threads at probability 0.5 both outcomes are overwhelmingly likely to occur;
    /// the assertion that matters is that neither aborted the process.
    EXPECT_EQ(threw + constructed, 200u);
    EXPECT_GT(threw, 0u) << "no construction failed, so the teardown path was never exercised";
}

/// A huge delay must not wrap into a past deadline and fire immediately.
TEST(BackgroundSchedulePool, ScheduleAfterHugeDelayDoesNotFire)
{
    auto pool = BackgroundSchedulePool::create(4, 4, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::atomic<size_t> executions = 0;
    BackgroundSchedulePoolTaskHolder task;
    task = pool->createTask(StorageID::createEmpty(), "huge_delay", [&] { ++executions; });
    ASSERT_EQ(task->activate(), true);

    /// The first value converts to microseconds losslessly, so it reaches the wait and overflows
    /// there when it is widened to nanoseconds. The others already wrap (mod 2^64) during the
    /// conversion, so they only ever exercise the deadline arithmetic.
    for (size_t ms : {size_t(10000000000000), size_t(1) << 63, std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max() / 1000 + 1})
    {
        ASSERT_EQ(task->scheduleAfter(ms), true);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        /// Assert the task is parked, not merely unfired: a scheduler that dropped the request
        /// entirely would also leave `executions` at zero.
        size_t found = 0;
        for (const auto & info : pool->getTasks())
        {
            if (info.log_name != "huge_delay")
                continue;
            ++found;
            ASSERT_TRUE(info.delayed);
            ASSERT_FALSE(info.executing);
            ASSERT_FALSE(info.scheduled);
        }
        ASSERT_EQ(found, 1u);
        ASSERT_EQ(executions.load(), 0u);
    }

    ASSERT_EQ(task->deactivate(), true);
    pool->join();
}

/// Raising the pool size at runtime (config reload) must also raise the number of tasks of one
/// type that may run at once, including for a group already paused at the old limit.
TEST(BackgroundSchedulePool, IncreaseThreadsCountRaisesSameTypeLimit)
{
    constexpr size_t initial_threads = 2;
    constexpr size_t increased_threads = 4;
    constexpr size_t num_tasks = increased_threads;

    auto pool = BackgroundSchedulePool::create(initial_threads, initial_threads, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::mutex mutex;
    std::condition_variable running_changed;
    std::condition_variable release_tasks;
    size_t running = 0;
    bool release = false;

    /// A single lambda expression ⇒ one closure type ⇒ all tasks share one task group.
    auto task_body = [&]
    {
        std::unique_lock lock(mutex);
        ++running;
        running_changed.notify_all();
        release_tasks.wait(lock, [&] { return release; });
    };

    std::vector<BackgroundSchedulePoolTaskHolder> tasks;
    tasks.reserve(num_tasks);
    for (size_t i = 0; i < num_tasks; ++i)
        tasks.emplace_back(pool->createTask(StorageID::createEmpty(), "increase_same_type", task_body));

    for (auto & task : tasks)
        EXPECT_TRUE(task->activateAndSchedule());

    bool initial_ran = false;
    bool all_ran = false;
    size_t running_before_increase = 0;
    size_t running_after_increase = 0;
    {
        std::unique_lock lock(mutex);
        initial_ran = running_changed.wait_for(lock, std::chrono::seconds(60), [&] { return running >= initial_threads; });
        running_before_increase = running;
        if (initial_ran)
        {
            lock.unlock();
            pool->increaseThreadsCount(increased_threads);
            lock.lock();
            all_ran = running_changed.wait_for(lock, std::chrono::seconds(60), [&] { return running >= num_tasks; });
        }
        running_after_increase = running;
        release = true;
        release_tasks.notify_all();
    }

    /// The pool's destructor requires join(), so a fatal assertion must not precede it.
    pool->join();

    EXPECT_TRUE(initial_ran);
    EXPECT_EQ(running_before_increase, initial_threads);
    EXPECT_TRUE(all_ran) << "only " << running_after_increase << " of " << num_tasks << " tasks of one type run after raising the pool size";
    EXPECT_EQ(running_after_increase, num_tasks);
}

/// With a ratio, the raised limit is the new size times the ratio, exactly as at startup.
TEST(BackgroundSchedulePool, IncreaseThreadsCountKeepsPerTypeRatio)
{
    constexpr size_t initial_threads = 4;
    constexpr size_t increased_threads = 8;
    constexpr double ratio = 0.5;
    constexpr size_t initial_limit = 2;
    constexpr size_t increased_limit = 4;
    constexpr size_t num_tasks = 6;

    auto pool = BackgroundSchedulePool::create(initial_threads, initial_threads, ratio, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::mutex mutex;
    std::condition_variable running_changed;
    std::condition_variable release_tasks;
    size_t running = 0;
    bool release = false;

    /// A single lambda expression ⇒ one closure type ⇒ all tasks share one task group.
    auto task_body = [&]
    {
        std::unique_lock lock(mutex);
        ++running;
        running_changed.notify_all();
        release_tasks.wait(lock, [&] { return release; });
    };

    std::vector<BackgroundSchedulePoolTaskHolder> tasks;
    tasks.reserve(num_tasks);
    for (size_t i = 0; i < num_tasks; ++i)
        tasks.emplace_back(pool->createTask(StorageID::createEmpty(), "increase_keeps_ratio", task_body));

    for (auto & task : tasks)
        EXPECT_TRUE(task->activateAndSchedule());

    bool initial_ran = false;
    bool increased_ran = false;
    size_t running_before_increase = 0;
    size_t running_after_increase = 0;
    size_t running_later = 0;
    {
        std::unique_lock lock(mutex);
        initial_ran = running_changed.wait_for(lock, std::chrono::seconds(60), [&] { return running >= initial_limit; });
        running_before_increase = running;
        if (initial_ran)
        {
            lock.unlock();
            pool->increaseThreadsCount(increased_threads);
            lock.lock();
            /// The pool still has idle workers here, so they must be woken to take the extra tasks.
            increased_ran = running_changed.wait_for(lock, std::chrono::seconds(60), [&] { return running >= increased_limit; });
            running_after_increase = running;
            if (increased_ran)
            {
                lock.unlock();
                std::this_thread::sleep_for(std::chrono::seconds(1));
                lock.lock();
                running_later = running;
            }
        }
        release = true;
        release_tasks.notify_all();
    }

    /// The pool's destructor requires join(), so a fatal assertion must not precede it.
    pool->join();

    EXPECT_TRUE(initial_ran);
    EXPECT_EQ(running_before_increase, initial_limit);
    EXPECT_TRUE(increased_ran) << "only " << running_after_increase << " of " << increased_limit << " tasks of one type run after raising the pool size";
    EXPECT_EQ(running_after_increase, increased_limit);
    EXPECT_EQ(running_later, increased_limit) << "the per-type ratio is not kept after raising the pool size";
}

/// The recomputed limit can be lower than the current one (0.4 * 2 truncates to 0, which means the
/// pool size 2, while 0.4 * 3 gives 1). It must be ignored: a second wave of tasks of the type,
/// scheduled after the resize, must still run two at once.
TEST(BackgroundSchedulePool, IncreaseThreadsCountNeverLowersPerTypeLimit)
{
    constexpr size_t initial_threads = 2;
    constexpr size_t increased_threads = 3;
    constexpr double ratio = 0.4;
    constexpr size_t limit = 2;
    constexpr size_t num_tasks = 2 * limit;

    auto pool = BackgroundSchedulePool::create(initial_threads, initial_threads, ratio, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);

    std::mutex mutex;
    std::condition_variable state_changed;
    size_t started = 0;
    size_t running = 0;
    size_t released = 0;
    size_t finished = 0;

    /// A single lambda expression ⇒ one closure type ⇒ all tasks share one task group.
    auto task_body = [&]
    {
        std::unique_lock lock(mutex);
        const size_t ordinal = started++;
        ++running;
        state_changed.notify_all();
        state_changed.wait(lock, [&] { return ordinal < released; });
        --running;
        ++finished;
        state_changed.notify_all();
    };

    std::vector<BackgroundSchedulePoolTaskHolder> tasks;
    tasks.reserve(num_tasks);
    for (size_t i = 0; i < num_tasks; ++i)
        tasks.emplace_back(pool->createTask(StorageID::createEmpty(), "increase_never_lowers", task_body));

    for (size_t i = 0; i < limit; ++i)
        EXPECT_TRUE(tasks[i]->activateAndSchedule());

    bool first_wave_ran = false;
    bool first_wave_finished = false;
    bool second_wave_ran = false;
    size_t running_before_increase = 0;
    size_t running_in_second_wave = 0;
    {
        std::unique_lock lock(mutex);
        first_wave_ran = state_changed.wait_for(lock, std::chrono::seconds(60), [&] { return running >= limit; });
        running_before_increase = running;
        if (first_wave_ran)
        {
            lock.unlock();
            pool->increaseThreadsCount(increased_threads);
            lock.lock();
            released = limit;
            state_changed.notify_all();
            first_wave_finished = state_changed.wait_for(lock, std::chrono::seconds(60), [&] { return finished >= limit; });
        }
        if (first_wave_finished)
        {
            lock.unlock();
            /// The pool decrements its running count after the task function returns.
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
            while (!pool->getTasks().empty() && std::chrono::steady_clock::now() < deadline)
                std::this_thread::yield();
            for (size_t i = limit; i < num_tasks; ++i)
                EXPECT_TRUE(tasks[i]->activateAndSchedule());
            lock.lock();
            second_wave_ran = state_changed.wait_for(lock, std::chrono::seconds(60), [&] { return running >= limit; });
            running_in_second_wave = running;
        }
        released = num_tasks;
        state_changed.notify_all();
    }

    /// The pool's destructor requires join(), so a fatal assertion must not precede it.
    pool->join();

    EXPECT_TRUE(first_wave_ran);
    EXPECT_EQ(running_before_increase, limit);
    EXPECT_TRUE(first_wave_finished);
    EXPECT_TRUE(second_wave_ran) << "only " << running_in_second_wave << " of " << limit << " tasks of one type run after raising the pool size";
    EXPECT_EQ(running_in_second_wave, limit);
}
