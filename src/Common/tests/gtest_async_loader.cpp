#include <boost/core/noncopyable.hpp>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstdlib>
#include <exception>
#include <list>
#include <barrier>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string_view>
#include <vector>
#include <thread>
#include <pcg_random.hpp>

#include <base/types.h>
#include <base/sleep.h>
#include <Common/Exception.h>
#include <Common/AsyncLoader.h>
#include <Common/ProfileEvents.h>
#include <Common/randomSeed.h>
#include <Common/ThreadPool.h>

using namespace DB;

namespace
{

// Looked up by name, so this file links whether or not the event is registered. 0 when it is not.
UInt64 spawnFailures()
{
    static const ProfileEvents::Event event = []
    {
        for (auto e = ProfileEvents::Event(0); e < ProfileEvents::end(); ++e)
            if (ProfileEvents::getName(e) == "AsyncLoaderSpawnFailures")
                return e;
        return ProfileEvents::end();
    }();

    return event == ProfileEvents::end() ? 0 : ProfileEvents::global_counters[event];
}

}

namespace CurrentMetrics
{
    extern const Metric TablesLoaderBackgroundThreads;
    extern const Metric TablesLoaderBackgroundThreadsActive;
    extern const Metric TablesLoaderBackgroundThreadsScheduled;
}

namespace DB::ErrorCodes
{
    extern const int ASYNC_LOAD_CYCLE;
    extern const int ASYNC_LOAD_FAILED;
    extern const int ASYNC_LOAD_CANCELED;
    extern const int ASYNC_LOAD_WAIT_FAILED;
}

struct Initializer {
    size_t max_threads = 1;
    Priority priority;
};

struct AsyncLoaderTest
{
    AsyncLoader loader;

    std::mutex rng_mutex;
    pcg64 rng{randomSeed()};

    explicit AsyncLoaderTest(std::vector<Initializer> initializers)
        : loader(getPoolInitializers(initializers), /* log_failures = */ false, /* log_progress = */ false, /* log_events = */ false)
    {
        loader.pause(); // All tests call `unpause()` manually to better control ordering
    }

    explicit AsyncLoaderTest(size_t max_threads = 1)
        : AsyncLoaderTest({{.max_threads = max_threads, .priority = {}}})
    {}

    std::vector<AsyncLoader::PoolInitializer> getPoolInitializers(std::vector<Initializer> initializers)
    {
        std::vector<AsyncLoader::PoolInitializer> result;
        size_t pool_id = 0;
        for (auto & desc : initializers)
        {
            result.push_back({
                .name = fmt::format("Pool{}", pool_id),
                .metric_threads = CurrentMetrics::TablesLoaderBackgroundThreads,
                .metric_active_threads = CurrentMetrics::TablesLoaderBackgroundThreadsActive,
                .metric_scheduled_threads = CurrentMetrics::TablesLoaderBackgroundThreadsScheduled,
                .max_threads = desc.max_threads,
                .priority = desc.priority
            });
            pool_id++;
        }
        return result;
    }

    template <typename T>
    T randomInt(T from, T to)
    {
        std::uniform_int_distribution<T> distribution(from, to);
        std::lock_guard lock(rng_mutex);
        return distribution(rng);
    }

    void randomSleepUs(UInt64 min_us, UInt64 max_us, int probability_percent)
    {
        if (randomInt(0, 99) < probability_percent)
            std::this_thread::sleep_for(std::chrono::microseconds(randomInt(min_us, max_us)));
    }

    template <typename JobFunc>
    LoadJobSet randomJobSet(int job_count, int dep_probability_percent, JobFunc job_func, std::string_view name_prefix = "job")
    {
        std::vector<LoadJobPtr> jobs;
        jobs.reserve(job_count);
        for (int j = 0; j < job_count; j++)
        {
            LoadJobSet deps;
            for (int d = 0; d < j; d++)
            {
                if (randomInt(0, 99) < dep_probability_percent)
                    deps.insert(jobs[d]);
            }
            jobs.push_back(makeLoadJob(std::move(deps), fmt::format("{}{}", name_prefix, j), job_func));
        }
        return {jobs.begin(), jobs.end()};
    }

    template <typename JobFunc>
    LoadJobSet randomJobSet(int job_count, int dep_probability_percent, const std::vector<LoadJobPtr> & external_deps, JobFunc job_func, std::string_view name_prefix = "job")
    {
        std::vector<LoadJobPtr> jobs;
        jobs.reserve(job_count);
        for (int j = 0; j < job_count; j++)
        {
            LoadJobSet deps;
            for (int d = 0; d < j; d++)
            {
                if (randomInt(0, 99) < dep_probability_percent)
                    deps.insert(jobs[d]);
            }
            if (!external_deps.empty() && randomInt(0, 99) < dep_probability_percent)
                deps.insert(external_deps[randomInt<size_t>(0, external_deps.size() - 1)]);
            jobs.push_back(makeLoadJob(std::move(deps), fmt::format("{}{}", name_prefix, j), job_func));
        }
        return {jobs.begin(), jobs.end()};
    }

    template <typename JobFunc>
    LoadJobSet chainJobSet(int job_count, JobFunc job_func, std::string_view name_prefix = "job")
    {
        std::vector<LoadJobPtr> jobs;
        jobs.reserve(job_count);
        jobs.push_back(makeLoadJob({}, fmt::format("{}{}", name_prefix, 0), job_func));
        for (int j = 1; j < job_count; j++)
            jobs.push_back(makeLoadJob({ jobs[j - 1] }, fmt::format("{}{}", name_prefix, j), job_func));
        return {jobs.begin(), jobs.end()};
    }

    LoadTaskPtr schedule(LoadJobSet && jobs)
    {
        LoadTaskPtr task = makeLoadTask(loader, std::move(jobs));
        task->schedule();
        return task;
    }
};

TEST(AsyncLoader, Smoke)
{
    AsyncLoaderTest t({
        {.max_threads = 2, .priority = Priority{0}},
        {.max_threads = 2, .priority = Priority{1}},
    });

    static constexpr size_t low_priority_pool = 1;

    std::atomic<size_t> jobs_done{0};
    std::atomic<size_t> low_priority_jobs_done{0};

    auto job_func = [&] (AsyncLoader &, const LoadJobPtr & self) {
        jobs_done++;
        if (self->pool() == low_priority_pool)
            low_priority_jobs_done++;
    };

    {
        auto job1 = makeLoadJob({}, "job1", job_func);
        auto job2 = makeLoadJob({ job1 }, "job2", job_func);
        auto task1 = t.schedule({ job1, job2 });

        auto job3 = makeLoadJob({ job2 }, "job3", job_func);
        auto job4 = makeLoadJob({ job2 }, "job4", job_func);
        auto task2 = t.schedule({ job3, job4 });
        auto job5 = makeLoadJob({ job3, job4 }, low_priority_pool, "job5", job_func);
        task2->merge(t.schedule({ job5 }));

        std::thread waiter_thread([&t, job5] { t.loader.wait(job5); });

        t.loader.unpause();

        t.loader.wait(job3);
        t.loader.wait();
        t.loader.wait(job4);

        waiter_thread.join();

        ASSERT_EQ(job1->status(), LoadStatus::OK);
        ASSERT_EQ(job2->status(), LoadStatus::OK);
    }

    ASSERT_EQ(jobs_done, 5);
    ASSERT_EQ(low_priority_jobs_done, 1);

    t.loader.pause();
}

TEST(AsyncLoader, CycleDetection)
{
    AsyncLoaderTest t;

    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &) {};

    LoadJobPtr cycle_breaker; // To avoid memleak we introduce with a cycle

    try
    {
        std::vector<LoadJobPtr> jobs;
        jobs.reserve(16);
        jobs.push_back(makeLoadJob({}, "job0", job_func));
        jobs.push_back(makeLoadJob({ jobs[0] }, "job1", job_func));
        jobs.push_back(makeLoadJob({ jobs[0], jobs[1] }, "job2", job_func));
        jobs.push_back(makeLoadJob({ jobs[0], jobs[2] }, "job3", job_func));

        // Actually it is hard to construct a cycle, but suppose someone was able to succeed violating constness
        const_cast<LoadJobSet &>(jobs[1]->dependencies).insert(jobs[3]);
        cycle_breaker = jobs[1];

        // Add couple unrelated jobs
        jobs.push_back(makeLoadJob({ jobs[1] }, "job4", job_func));
        jobs.push_back(makeLoadJob({ jobs[4] }, "job5", job_func));
        jobs.push_back(makeLoadJob({ jobs[3] }, "job6", job_func));
        jobs.push_back(makeLoadJob({ jobs[1], jobs[2], jobs[3], jobs[4], jobs[5], jobs[6] }, "job7", job_func));

        // Also add another not connected jobs
        jobs.push_back(makeLoadJob({}, "job8", job_func));
        jobs.push_back(makeLoadJob({}, "job9", job_func));
        jobs.push_back(makeLoadJob({ jobs[9] }, "job10", job_func));

        auto task1 = t.schedule({ jobs.begin(), jobs.end()});
        FAIL();
    }
    catch (Exception & e)
    {
        int present[] = { 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0 };
        for (int i = 0; i < std::size(present); i++)
            ASSERT_EQ(e.message().find(fmt::format("job{}", i)) != String::npos, present[i]);
    }

    const_cast<LoadJobSet &>(cycle_breaker->dependencies).clear();
}

TEST(AsyncLoader, CancelPendingJob)
{
    AsyncLoaderTest t;

    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &) {};

    auto job = makeLoadJob({}, "job", job_func);
    auto task = t.schedule({ job });

    task->remove(); // this cancels pending the job (async loader was not started to execute it)

    ASSERT_EQ(job->status(), LoadStatus::CANCELED);
    try
    {
        t.loader.wait(job);
        FAIL();
    }
    catch (Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::ASYNC_LOAD_WAIT_FAILED);
        ASSERT_TRUE(e.message().contains("ASYNC_LOAD_CANCELED"));
    }
}

TEST(AsyncLoader, CancelPendingTask)
{
    AsyncLoaderTest t;

    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &) {};

    auto job1 = makeLoadJob({}, "job1", job_func);
    auto job2 = makeLoadJob({ job1 }, "job2", job_func);
    auto task = t.schedule({ job1, job2 });

    task->remove(); // this cancels both jobs (async loader was not started to execute it)

    ASSERT_EQ(job1->status(), LoadStatus::CANCELED);
    ASSERT_EQ(job2->status(), LoadStatus::CANCELED);

    try
    {
        t.loader.wait(job1);
        FAIL();
    }
    catch (Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::ASYNC_LOAD_WAIT_FAILED);
        ASSERT_TRUE(e.message().contains("ASYNC_LOAD_CANCELED"));
    }

    try
    {
        t.loader.wait(job2);
        FAIL();
    }
    catch (Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::ASYNC_LOAD_WAIT_FAILED);
        ASSERT_TRUE(e.message().contains("ASYNC_LOAD_CANCELED"));
    }
}

TEST(AsyncLoader, CancelPendingDependency)
{
    AsyncLoaderTest t;

    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &) {};

    auto job1 = makeLoadJob({}, "job1", job_func);
    auto job2 = makeLoadJob({ job1 }, "job2", job_func);
    auto task1 = t.schedule({ job1 });
    auto task2 = t.schedule({ job2 });

    task1->remove(); // this cancels both jobs, due to dependency (async loader was not started to execute it)

    ASSERT_EQ(job1->status(), LoadStatus::CANCELED);
    ASSERT_EQ(job2->status(), LoadStatus::CANCELED);

    try
    {
        t.loader.wait(job1);
        FAIL();
    }
    catch (Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::ASYNC_LOAD_WAIT_FAILED);
        ASSERT_TRUE(e.message().contains("ASYNC_LOAD_CANCELED"));
    }

    try
    {
        t.loader.wait(job2);
        FAIL();
    }
    catch (Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::ASYNC_LOAD_WAIT_FAILED);
        ASSERT_TRUE(e.message().contains("ASYNC_LOAD_CANCELED"));
    }
}

TEST(AsyncLoader, CancelExecutingJob)
{
    AsyncLoaderTest t;
    t.loader.unpause();

    std::barrier<std::__empty_completion> sync(2);

    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        sync.arrive_and_wait(); // (A) sync with main thread
        sync.arrive_and_wait(); // (B) wait for waiter
        // signals (C)
    };

    auto job = makeLoadJob({}, "job", job_func);
    auto task = t.schedule({ job });

    sync.arrive_and_wait(); // (A) wait for job to start executing
    std::thread canceler([&]
    {
        task->remove(); // waits for (C)
    });
    while (job->waitersCount() == 0)
        std::this_thread::yield();
    ASSERT_EQ(job->status(), LoadStatus::PENDING);
    sync.arrive_and_wait(); // (B) sync with job
    canceler.join();

    ASSERT_EQ(job->status(), LoadStatus::OK);
    t.loader.wait(job);
}

TEST(AsyncLoader, CancelExecutingTask)
{
    AsyncLoaderTest t(16);
    t.loader.unpause();
    std::barrier<std::__empty_completion> sync(2);

    auto blocker_job_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        sync.arrive_and_wait(); // (A) sync with main thread
        sync.arrive_and_wait(); // (B) wait for waiter
        // signals (C)
    };

    auto job_to_cancel_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        FAIL(); // this job should be canceled
    };

    auto job_to_succeed_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
    };

    // Make several iterations to catch the race (if any)
    for (int iteration = 0; iteration < 10; iteration++) {
        std::vector<LoadJobPtr> task1_jobs;
        task1_jobs.reserve(256);
        auto blocker_job = makeLoadJob({}, "blocker_job", blocker_job_func);
        task1_jobs.push_back(blocker_job);
        for (int i = 0; i < 100; i++)
            task1_jobs.push_back(makeLoadJob({ blocker_job }, "job_to_cancel", job_to_cancel_func));
        auto task1 = t.schedule({ task1_jobs.begin(), task1_jobs.end() });
        auto job_to_succeed = makeLoadJob({ blocker_job }, "job_to_succeed", job_to_succeed_func);
        auto task2 = t.schedule({ job_to_succeed });

        sync.arrive_and_wait(); // (A) wait for job to start executing
        std::thread canceler([&]
        {
            task1->remove(); // waits for (C)
        });
        while (blocker_job->waitersCount() == 0)
            std::this_thread::yield();
        ASSERT_EQ(blocker_job->status(), LoadStatus::PENDING);
        sync.arrive_and_wait(); // (B) sync with job
        canceler.join();
        t.loader.wait();

        ASSERT_EQ(blocker_job->status(), LoadStatus::OK);
        ASSERT_EQ(job_to_succeed->status(), LoadStatus::OK);
        for (const auto & job : task1_jobs)
        {
            if (job != blocker_job)
                ASSERT_EQ(job->status(), LoadStatus::CANCELED);
        }
    }
}

TEST(AsyncLoader, JobFailure)
{
    AsyncLoaderTest t;
    t.loader.unpause();

    std::string error_message = "test job failure";

    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &) {
        throw std::runtime_error(error_message);
    };

    auto job = makeLoadJob({}, "job", job_func);
    auto task = t.schedule({ job });

    t.loader.wait();

    ASSERT_EQ(job->status(), LoadStatus::FAILED);
    try
    {
        t.loader.wait(job);
        FAIL();
    }
    catch (Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::ASYNC_LOAD_WAIT_FAILED);
        ASSERT_TRUE(e.message().contains(error_message));
        ASSERT_TRUE(e.message().contains("ASYNC_LOAD_FAILED"));
    }
}

TEST(AsyncLoader, ScheduleJobWithFailedDependencies)
{
    AsyncLoaderTest t;
    t.loader.unpause();

    std::string_view error_message = "test job failure";

    auto failed_job_func = [&] (AsyncLoader &, const LoadJobPtr &) {
        throw Exception(ErrorCodes::ASYNC_LOAD_FAILED, "{}", error_message);
    };

    auto failed_job = makeLoadJob({}, "failed_job", failed_job_func);
    auto failed_task = t.schedule({ failed_job });

    t.loader.wait();

    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &) {};

    auto job1 = makeLoadJob({ failed_job }, "job1", job_func);
    auto job2 = makeLoadJob({ job1 }, "job2", job_func);
    auto task = t.schedule({ job1, job2 });

    t.loader.wait();

    ASSERT_EQ(job1->status(), LoadStatus::CANCELED);
    ASSERT_EQ(job2->status(), LoadStatus::CANCELED);
    try
    {
        t.loader.wait(job1);
        FAIL();
    }
    catch (Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::ASYNC_LOAD_WAIT_FAILED);
        ASSERT_TRUE(e.message().contains("ASYNC_LOAD_CANCELED"));
        ASSERT_TRUE(e.message().contains(error_message));
    }
    try
    {
        t.loader.wait(job2);
        FAIL();
    }
    catch (Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::ASYNC_LOAD_WAIT_FAILED);
        ASSERT_TRUE(e.message().contains("ASYNC_LOAD_CANCELED"));
        ASSERT_TRUE(e.message().contains(error_message));
    }
}

TEST(AsyncLoader, ScheduleJobWithCanceledDependencies)
{
    AsyncLoaderTest t;

    auto canceled_job_func = [&] (AsyncLoader &, const LoadJobPtr &) {};
    auto canceled_job = makeLoadJob({}, "canceled_job", canceled_job_func);
    auto canceled_task = t.schedule({ canceled_job });
    canceled_task->remove();

    t.loader.unpause();

    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &) {};
    auto job1 = makeLoadJob({ canceled_job }, "job1", job_func);
    auto job2 = makeLoadJob({ job1 }, "job2", job_func);
    auto task = t.schedule({ job1, job2 });

    t.loader.wait();

    ASSERT_EQ(job1->status(), LoadStatus::CANCELED);
    ASSERT_EQ(job2->status(), LoadStatus::CANCELED);
    try
    {
        t.loader.wait(job1);
        FAIL();
    }
    catch (Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::ASYNC_LOAD_WAIT_FAILED);
        ASSERT_TRUE(e.message().contains("ASYNC_LOAD_CANCELED"));
    }
    try
    {
        t.loader.wait(job2);
        FAIL();
    }
    catch (Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::ASYNC_LOAD_WAIT_FAILED);
        ASSERT_TRUE(e.message().contains("ASYNC_LOAD_CANCELED"));
    }
}

TEST(AsyncLoader, IgnoreDependencyFailure)
{
    AsyncLoaderTest t;
    std::atomic<bool> success{false};
    t.loader.unpause();

    std::string_view error_message = "test job failure";

    auto failed_job_func = [&] (AsyncLoader &, const LoadJobPtr &) {
        throw Exception(ErrorCodes::ASYNC_LOAD_FAILED, "{}", error_message);
    };
    auto dependent_job_func = [&] (AsyncLoader &, const LoadJobPtr &) {
        success.store(true);
    };

    auto failed_job = makeLoadJob({}, "failed_job", failed_job_func);
    auto dependent_job = makeLoadJob({failed_job},
        "dependent_job", ignoreDependencyFailure, dependent_job_func);
    auto task = t.schedule({ failed_job, dependent_job });

    t.loader.wait();

    ASSERT_EQ(failed_job->status(), LoadStatus::FAILED);
    ASSERT_EQ(dependent_job->status(), LoadStatus::OK);
    ASSERT_EQ(success.load(), true);
}

TEST(AsyncLoader, CustomDependencyFailure)
{
    AsyncLoaderTest t(16);
    int error_count = 0;
    std::atomic<size_t> good_count{0};
    std::barrier<std::__empty_completion> canceled_sync(4);
    t.loader.unpause();

    std::string_view error_message = "test job failure";

    auto evil_dep_func = [&] (AsyncLoader &, const LoadJobPtr &) {
        throw Exception(ErrorCodes::ASYNC_LOAD_FAILED, "{}", error_message);
    };
    auto good_dep_func = [&] (AsyncLoader &, const LoadJobPtr &) {
        good_count++;
    };
    auto late_dep_func = [&] (AsyncLoader &, const LoadJobPtr &) {
        canceled_sync.arrive_and_wait(); // wait for fail (A) before this job is finished
    };
    auto collect_job_func = [&] (AsyncLoader &, const LoadJobPtr &) {
        FAIL(); // job should be canceled, so we never get here
    };
    auto dependent_job_func = [&] (AsyncLoader &, const LoadJobPtr &) {
        FAIL(); // job should be canceled, so we never get here
    };
    auto fail_after_two = [&] (const LoadJobPtr & self, const LoadJobPtr &, std::exception_ptr & cancel) {
        if (++error_count == 2)
            cancel = std::make_exception_ptr(Exception(ErrorCodes::ASYNC_LOAD_CANCELED,
                "Load job '{}' canceled: too many dependencies have failed",
                self->name));
    };

    auto evil_dep1 = makeLoadJob({}, "evil_dep1", evil_dep_func);
    auto evil_dep2 = makeLoadJob({}, "evil_dep2", evil_dep_func);
    auto evil_dep3 = makeLoadJob({}, "evil_dep3", evil_dep_func);
    auto good_dep1 = makeLoadJob({}, "good_dep1", good_dep_func);
    auto good_dep2 = makeLoadJob({}, "good_dep2", good_dep_func);
    auto good_dep3 = makeLoadJob({}, "good_dep3", good_dep_func);
    auto late_dep1 = makeLoadJob({}, "late_dep1", late_dep_func);
    auto late_dep2 = makeLoadJob({}, "late_dep2", late_dep_func);
    auto late_dep3 = makeLoadJob({}, "late_dep3", late_dep_func);
    auto collect_job = makeLoadJob({
            evil_dep1, evil_dep2, evil_dep3,
            good_dep1, good_dep2, good_dep3,
            late_dep1, late_dep2, late_dep3
        }, "collect_job", fail_after_two, collect_job_func);
    auto dependent_job1 = makeLoadJob({ collect_job }, "dependent_job1", dependent_job_func);
    auto dependent_job2 = makeLoadJob({ collect_job }, "dependent_job2", dependent_job_func);
    auto dependent_job3 = makeLoadJob({ collect_job }, "dependent_job3", dependent_job_func);
    auto task = t.schedule({
            dependent_job1, dependent_job2, dependent_job3,
            collect_job,
            late_dep1, late_dep2, late_dep3,
            good_dep1, good_dep2, good_dep3,
            evil_dep1, evil_dep2, evil_dep3,
        });

    t.loader.wait(collect_job, true);
    canceled_sync.arrive_and_wait(); // (A)

    t.loader.wait();

    ASSERT_EQ(late_dep1->status(), LoadStatus::OK);
    ASSERT_EQ(late_dep2->status(), LoadStatus::OK);
    ASSERT_EQ(late_dep3->status(), LoadStatus::OK);
    ASSERT_EQ(collect_job->status(), LoadStatus::CANCELED);
    ASSERT_EQ(dependent_job1->status(), LoadStatus::CANCELED);
    ASSERT_EQ(dependent_job2->status(), LoadStatus::CANCELED);
    ASSERT_EQ(dependent_job3->status(), LoadStatus::CANCELED);
    ASSERT_EQ(good_count.load(), 3);
}

TEST(AsyncLoader, WaitersLimit)
{
    AsyncLoaderTest t(16);

    std::atomic<int> waiters_total{0};
    int waiters_limit = 5;
    auto waiters_inc = [&] (const LoadJobPtr &) {
        int value = waiters_total.load();
        while (true)
        {
            if (value >= waiters_limit)
                throw Exception(ErrorCodes::ASYNC_LOAD_FAILED, "Too many waiters: {}", value);
            if (waiters_total.compare_exchange_strong(value, value + 1))
                break;
        }
    };
    auto waiters_dec = [&] (const LoadJobPtr &) {
        waiters_total.fetch_sub(1);
    };

    std::barrier<std::__empty_completion> sync(2);
    t.loader.unpause();

    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &) {
        sync.arrive_and_wait(); // (A)
    };

    auto job = makeLoadJob({}, "job", waiters_inc, waiters_dec, job_func);
    auto task = t.schedule({job});

    std::atomic<int> failure{0};
    std::atomic<int> success{0};
    std::vector<std::thread> waiters;
    waiters.reserve(10);
    auto waiter = [&] {
        try
        {
            t.loader.wait(job);
            success.fetch_add(1);
        }
        catch(...) // Ok: test counts success/failure outcomes
        {
            failure.fetch_add(1);
        }
    };

    for (int i = 0; i < 10; i++)
        waiters.emplace_back(waiter);

    while (failure.load() != 5)
        std::this_thread::yield();

    ASSERT_EQ(job->waitersCount(), 5);

    sync.arrive_and_wait(); // (A)

    for (auto & thread : waiters)
        thread.join();

    ASSERT_EQ(success.load(), 5);
    ASSERT_EQ(failure.load(), 5);
    ASSERT_EQ(waiters_total.load(), 0);

    t.loader.wait();
}

TEST(AsyncLoader, TestConcurrency)
{
    AsyncLoaderTest t(10);
    t.loader.unpause();

    for (int concurrency = 1; concurrency <= 10; concurrency++)
    {
        std::barrier<std::__empty_completion> sync(concurrency);

        std::atomic<int> executing{0};
        auto job_func = [&] (AsyncLoader &, const LoadJobPtr &)
        {
            executing++;
            ASSERT_LE(executing, concurrency);
            sync.arrive_and_wait();
            executing--;
        };

        std::vector<LoadTaskPtr> tasks;
        tasks.reserve(concurrency);
        for (int i = 0; i < concurrency; i++)
            tasks.push_back(t.schedule(t.chainJobSet(5, job_func)));
        t.loader.wait();
        ASSERT_EQ(executing, 0);
    }
}

TEST(AsyncLoader, TestOverload)
{
    AsyncLoaderTest t(3);
    t.loader.unpause();

    size_t max_threads = t.loader.getMaxThreads(/* pool = */ 0);
    std::atomic<int> executing{0};

    for (int concurrency = 4; concurrency <= 8; concurrency++)
    {
        auto job_func = [&] (AsyncLoader &, const LoadJobPtr &)
        {
            executing++;
            t.randomSleepUs(100, 200, 100);
            ASSERT_LE(executing, max_threads);
            executing--;
        };

        t.loader.pause();
        std::vector<LoadTaskPtr> tasks;
        tasks.reserve(concurrency);
        for (int i = 0; i < concurrency; i++)
            tasks.push_back(t.schedule(t.chainJobSet(5, job_func)));
        t.loader.unpause();
        t.loader.wait();
        ASSERT_EQ(executing, 0);
    }
}

TEST(AsyncLoader, StaticPriorities)
{
    AsyncLoaderTest t({
        {.max_threads = 1, .priority{0}},
        {.max_threads = 1, .priority{-1}},
        {.max_threads = 1, .priority{-2}},
        {.max_threads = 1, .priority{-3}},
        {.max_threads = 1, .priority{-4}},
        {.max_threads = 1, .priority{-5}},
        {.max_threads = 1, .priority{-6}},
        {.max_threads = 1, .priority{-7}},
        {.max_threads = 1, .priority{-8}},
        {.max_threads = 1, .priority{-9}},
    });

    std::string schedule;

    auto job_func = [&] (AsyncLoader &, const LoadJobPtr & self)
    {
        schedule += fmt::format("{}{}", self->name, self->pool());
    };

    // Job DAG with priorities. After priority inheritance from H9, jobs D9 and E9 can be
    // executed in undefined order (Tested further in DynamicPriorities)
    // A0(9) -+-> B3
    //        |
    //        `-> C4
    //        |
    //        `-> D1(9) -.
    //        |          +-> F0(9) --> G0(9) --> H9
    //        `-> E2(9) -'
    std::vector<LoadJobPtr> jobs;
    jobs.push_back(makeLoadJob({}, 0, "A", job_func)); // 0
    jobs.push_back(makeLoadJob({ jobs[0] }, 3, "B", job_func)); // 1
    jobs.push_back(makeLoadJob({ jobs[0] }, 4, "C", job_func)); // 2
    jobs.push_back(makeLoadJob({ jobs[0] }, 1, "D", job_func)); // 3
    jobs.push_back(makeLoadJob({ jobs[0] }, 2, "E", job_func)); // 4
    jobs.push_back(makeLoadJob({ jobs[3], jobs[4] }, 0, "F", job_func)); // 5
    jobs.push_back(makeLoadJob({ jobs[5] }, 0, "G", job_func)); // 6
    jobs.push_back(makeLoadJob({ jobs[6] }, 9, "H", job_func)); // 7
    auto task = t.schedule({ jobs.begin(), jobs.end() });

    t.loader.unpause();
    t.loader.wait();
    ASSERT_TRUE(schedule == "A9E9D9F9G9H9C4B3" || schedule == "A9D9E9F9G9H9C4B3");
}

TEST(AsyncLoader, SimplePrioritization)
{
    AsyncLoaderTest t({
        {.max_threads = 1, .priority{0}},
        {.max_threads = 1, .priority{-1}},
        {.max_threads = 1, .priority{-2}},
    });

    t.loader.unpause();

    std::atomic<int> executed{0}; // Number of previously executed jobs (to test execution order)
    LoadJobPtr job_to_prioritize;

    auto job_func_A_booster = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        ASSERT_EQ(executed++, 0);
        t.loader.prioritize(job_to_prioritize, 2);
    };

    auto job_func_B_tester = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        ASSERT_EQ(executed++, 2);
    };

    auto job_func_C_boosted = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        ASSERT_EQ(executed++, 1);
    };

    std::vector<LoadJobPtr> jobs;
    jobs.push_back(makeLoadJob({}, 1, "A", job_func_A_booster)); // 0
    jobs.push_back(makeLoadJob({jobs[0]}, 1, "B", job_func_B_tester)); // 1
    jobs.push_back(makeLoadJob({}, 0, "C", job_func_C_boosted)); // 2
    auto task = makeLoadTask(t.loader, { jobs.begin(), jobs.end() });

    job_to_prioritize = jobs[2]; // C

    scheduleLoad(task);
    waitLoad(task);
}

TEST(AsyncLoader, DynamicPriorities)
{
    AsyncLoaderTest t({
        {.max_threads = 1, .priority{0}},
        {.max_threads = 1, .priority{-1}},
        {.max_threads = 1, .priority{-2}},
        {.max_threads = 1, .priority{-3}},
        {.max_threads = 1, .priority{-4}},
        {.max_threads = 1, .priority{-5}},
        {.max_threads = 1, .priority{-6}},
        {.max_threads = 1, .priority{-7}},
        {.max_threads = 1, .priority{-8}},
        {.max_threads = 1, .priority{-9}},
    });

    for (bool prioritize : {false, true})
    {
        // Although all pools have max_threads=1, workers from different pools can run simultaneously just after `prioritize()` call
        std::barrier<std::__empty_completion> sync(2);
        bool wait_sync = prioritize;
        std::mutex schedule_mutex;
        std::string schedule;

        LoadJobPtr job_to_prioritize;

        // Order of execution of jobs D and E after prioritization is undefined, because it depend on `ready_seqno`
        // (Which depends on initial `schedule()` order, which in turn depend on `std::unordered_map` order)
        // So we have to obtain `ready_seqno` to be sure.
        UInt64 ready_seqno_D = 0;
        UInt64 ready_seqno_E = 0;

        auto job_func = [&] (AsyncLoader &, const LoadJobPtr & self)
        {
            {
                std::unique_lock lock{schedule_mutex};
                schedule += fmt::format("{}{}", self->name, self->executionPool());
            }

            if (prioritize && self->name == "C")
            {
                for (const auto & state : t.loader.getJobStates())
                {
                    if (state.job->name == "D")
                        ready_seqno_D = state.ready_seqno;
                    if (state.job->name == "E")
                        ready_seqno_E = state.ready_seqno;
                }

                // Jobs D and E should be enqueued at the moment
                ASSERT_LT(0, ready_seqno_D);
                ASSERT_LT(0, ready_seqno_E);

                // Dynamic prioritization G0 -> G9
                // Note that it will spawn concurrent worker in higher priority pool
                t.loader.prioritize(job_to_prioritize, 9);

                sync.arrive_and_wait(); // (A) wait for higher priority worker (B) to test they can be concurrent
            }

            if (wait_sync && (self->name == "D" || self->name == "E"))
            {
                wait_sync = false;
                sync.arrive_and_wait(); // (B)
            }
        };

        // Job DAG with initial priorities. During execution of C4, job G0 priority is increased to G9, postponing B3 job executing.
        // A0 -+-> B3
        //     |
        //     `-> C4
        //     |
        //     `-> D1 -.
        //     |       +-> F0 --> G0 --> H0
        //     `-> E2 -'
        std::vector<LoadJobPtr> jobs;
        jobs.push_back(makeLoadJob({}, 0, "A", job_func)); // 0
        jobs.push_back(makeLoadJob({ jobs[0] }, 3, "B", job_func)); // 1
        jobs.push_back(makeLoadJob({ jobs[0] }, 4, "C", job_func)); // 2
        jobs.push_back(makeLoadJob({ jobs[0] }, 1, "D", job_func)); // 3
        jobs.push_back(makeLoadJob({ jobs[0] }, 2, "E", job_func)); // 4
        jobs.push_back(makeLoadJob({ jobs[3], jobs[4] }, 0, "F", job_func)); // 5
        jobs.push_back(makeLoadJob({ jobs[5] }, 0, "G", job_func)); // 6
        jobs.push_back(makeLoadJob({ jobs[6] }, 0, "H", job_func)); // 7
        auto task = t.schedule({ jobs.begin(), jobs.end() });

        job_to_prioritize = jobs[6]; // G

        t.loader.unpause();
        t.loader.wait();
        t.loader.pause();

        if (prioritize)
        {
            if (ready_seqno_D < ready_seqno_E)
                ASSERT_EQ(schedule, "A4C4D9E9F9G9B3H0");
            else
                ASSERT_EQ(schedule, "A4C4E9D9F9G9B3H0");
        }
        else
            ASSERT_EQ(schedule, "A4C4B3E2D1F0G0H0");
    }
}

TEST(AsyncLoader, JobPrioritizedWhileWaited)
{
    AsyncLoaderTest t({
        {.max_threads = 2, .priority{0}},
        {.max_threads = 1, .priority{-1}},
    });

    std::barrier<std::__empty_completion> sync(2);

    LoadJobPtr job_to_wait; // and then to prioritize

    auto running_job_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        sync.arrive_and_wait();
    };

    auto dependent_job_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
    };

    auto waiting_job_func = [&] (AsyncLoader & loader, const LoadJobPtr &)
    {
        loader.wait(job_to_wait);
    };

    std::vector<LoadJobPtr> jobs;
    jobs.push_back(makeLoadJob({}, 0, "running", running_job_func));
    jobs.push_back(makeLoadJob({jobs[0]}, 0, "dependent", dependent_job_func));
    jobs.push_back(makeLoadJob({}, 0, "waiting", waiting_job_func));
    auto task = t.schedule({ jobs.begin(), jobs.end() });

    job_to_wait = jobs[1];

    t.loader.unpause();

    while (job_to_wait->waitersCount() == 0)
        std::this_thread::yield();

    ASSERT_EQ(t.loader.suspendedWorkersCount(0), 1);

    t.loader.prioritize(job_to_wait, 1);
    sync.arrive_and_wait();

    t.loader.wait();
    t.loader.pause();
    ASSERT_EQ(t.loader.suspendedWorkersCount(1), 0);
    ASSERT_EQ(t.loader.suspendedWorkersCount(0), 0);
}

TEST(AsyncLoader, RandomIndependentTasks)
{
    AsyncLoaderTest t(16);
    t.loader.unpause();

    auto job_func = [&] (AsyncLoader &, const LoadJobPtr & self)
    {
        for (const auto & dep : self->dependencies)
            ASSERT_EQ(dep->status(), LoadStatus::OK);
        t.randomSleepUs(100, 500, 5);
    };

    std::vector<LoadTaskPtr> tasks;
    tasks.reserve(512);
    for (int i = 0; i < 512; i++)
    {
        int job_count = t.randomInt(1, 32);
        tasks.push_back(t.schedule(t.randomJobSet(job_count, 5, job_func)));
        t.randomSleepUs(100, 900, 20); // avg=100us
    }
}

TEST(AsyncLoader, RandomDependentTasks)
{
    AsyncLoaderTest t(16);
    t.loader.unpause();

    std::mutex mutex;
    std::condition_variable cv;
    std::vector<LoadTaskPtr> tasks;
    std::vector<LoadJobPtr> all_jobs;

    auto job_func = [&] (AsyncLoader &, const LoadJobPtr & self)
    {
        for (const auto & dep : self->dependencies)
            ASSERT_EQ(dep->status(), LoadStatus::OK);
        cv.notify_one();
    };

    std::unique_lock lock{mutex};

    int tasks_left = 1000;
    tasks.reserve(tasks_left);
    while (tasks_left-- > 0)
    {
        cv.wait(lock, [&] { return t.loader.getScheduledJobCount() < 100; });

        // Add one new task
        int job_count = t.randomInt(1, 32);
        LoadJobSet jobs = t.randomJobSet(job_count, 5, all_jobs, job_func);
        all_jobs.insert(all_jobs.end(), jobs.begin(), jobs.end());
        tasks.push_back(t.schedule(std::move(jobs)));

        // Cancel random old task
        if (tasks.size() > 100)
            tasks.erase(tasks.begin() + t.randomInt<size_t>(0, tasks.size() - 1));
    }

    t.loader.wait();
}

TEST(AsyncLoader, SetMaxThreads)
{
    AsyncLoaderTest t(1);

    std::atomic<int> sync_index{0};
    std::atomic<int> executing{0};
    int max_threads_values[] = {1, 2, 3, 4, 5, 4, 3, 2, 1, 5, 10, 5, 1, 20, 1};
    std::vector<std::unique_ptr<std::barrier<>>> syncs;
    syncs.reserve(std::size(max_threads_values));
    for (int max_threads : max_threads_values)
        syncs.push_back(std::make_unique<std::barrier<>>(max_threads + 1));


    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        int idx = sync_index;
        if (idx < syncs.size())
        {
            executing++;
            syncs[idx]->arrive_and_wait(); // (A)
            executing--;
            syncs[idx]->arrive_and_wait(); // (B)
        }
    };

    // Generate enough independent jobs
    std::vector<LoadTaskPtr> tasks;
    tasks.reserve(1000);
    for (int i = 0; i < 1000; i++)
        tasks.push_back(t.schedule({makeLoadJob({}, "job", job_func)}));

    t.loader.unpause();
    while (sync_index < syncs.size())
    {
        // Wait for `max_threads` jobs to start executing
        int idx = sync_index;
        while (executing.load() != max_threads_values[idx])
        {
            ASSERT_LE(executing, max_threads_values[idx]);
            std::this_thread::yield();
        }

        // Allow all jobs to finish
        syncs[idx]->arrive_and_wait(); // (A)
        sync_index++;
        if (sync_index < syncs.size())
            t.loader.setMaxThreads(/* pool = */ 0, max_threads_values[sync_index]);
        syncs[idx]->arrive_and_wait(); // (B) this sync point is required to allow `executing` value to go back down to zero after we change number of workers
    }
    t.loader.wait();
}

TEST(AsyncLoader, SubJobs)
{
    AsyncLoaderTest t(1);
    t.loader.unpause();

    // An example of component with an asynchronous loading interface
    class MyComponent : boost::noncopyable {
    public:
        MyComponent(AsyncLoader & loader_, int jobs)
            : loader(loader_)
            , jobs_left(jobs)
        {}

        [[nodiscard]] LoadTaskPtr loadAsync()
        {
            auto job_func = [this] (AsyncLoader &, const LoadJobPtr &) {
                auto sub_job_func = [this] (AsyncLoader &, const LoadJobPtr &) {
                    --jobs_left;
                };
                LoadJobSet jobs;
                for (size_t j = 0; j < jobs_left; j++)
                    jobs.insert(makeLoadJob({}, fmt::format("sub job {}", j), sub_job_func));
                waitLoad(makeLoadTask(loader, std::move(jobs)));
            };
            auto job = makeLoadJob({}, "main job", job_func);
            return load_task = makeLoadTask(loader, { job });
        }

        bool isLoaded() const
        {
            return jobs_left == 0;
        }

    private:
        AsyncLoader & loader;
        std::atomic<int> jobs_left;
        // It is a good practice to keep load task inside the component:
        // 1) to make sure it outlives its load jobs;
        // 2) to avoid removing load jobs from `system.asynchronous_loader` while we use the component
        LoadTaskPtr load_task;
    };

    for (double jobs_per_thread : std::array{0.5, 1.0, 2.0})
    {
        for (size_t threads = 1; threads <= 32; threads *= 2)
        {
            t.loader.setMaxThreads(0, threads);
            std::list<MyComponent> components;
            LoadTaskPtrs tasks;
            size_t size = static_cast<size_t>(jobs_per_thread * static_cast<double>(threads));
            tasks.reserve(size);
            for (size_t j = 0; j < size; j++)
            {
                components.emplace_back(t.loader, 5);
                tasks.emplace_back(components.back().loadAsync());
            }
            waitLoad(tasks);
            for (const auto & component: components)
                ASSERT_TRUE(component.isLoaded());
        }
    }
}

TEST(AsyncLoader, RecursiveJob)
{
    AsyncLoaderTest t(1);
    t.loader.unpause();

    // An example of component with an asynchronous loading interface (a complicated one)
    class MyComponent : boost::noncopyable {
    public:
        MyComponent(AsyncLoader & loader_, int jobs)
            : loader(loader_)
            , jobs_left(jobs)
        {}

        [[nodiscard]] LoadTaskPtr loadAsync()
        {
            return load_task = loadAsyncImpl(jobs_left);
        }

        bool isLoaded() const
        {
            return jobs_left == 0;
        }

    private:
        [[nodiscard]] LoadTaskPtr loadAsyncImpl(int id)
        {
            auto job_func = [this] (AsyncLoader &, const LoadJobPtr & self) {
                jobFunction(self);
            };
            auto job = makeLoadJob({}, fmt::format("job{}", id), job_func);
            auto task = makeLoadTask(loader, { job });
            return task;
        }

        void jobFunction(const LoadJobPtr & self)
        {
            int next = --jobs_left;
            if (next > 0)
                waitLoad(self->pool(), loadAsyncImpl(next));
        }

        AsyncLoader & loader;
        std::atomic<int> jobs_left;
        // It is a good practice to keep load task inside the component:
        // 1) to make sure it outlives its load jobs;
        // 2) to avoid removing load jobs from `system.asynchronous_loader` while we use the component
        LoadTaskPtr load_task;
    };

    for (double jobs_per_thread : std::array{0.5, 1.0, 2.0})
    {
        for (size_t threads = 1; threads <= 32; threads *= 2)
        {
            t.loader.setMaxThreads(0, threads);
            std::list<MyComponent> components;
            LoadTaskPtrs tasks;
            size_t size = static_cast<size_t>(jobs_per_thread * static_cast<double>(threads));
            tasks.reserve(size);
            for (size_t j = 0; j < size; j++)
            {
                components.emplace_back(t.loader, 5);
                tasks.emplace_back(components.back().loadAsync());
            }
            waitLoad(tasks);
            for (const auto & component: components)
                ASSERT_TRUE(component.isLoaded());
        }
    }
}

// Restores the global thread pool limits whatever the test does, because exhausting them is
// observable by every other test in this binary.
struct GlobalThreadPoolLimits
{
    GlobalThreadPool & pool = GlobalThreadPool::instance();

    void saturate(size_t capacity)
    {
        pool.setMaxThreads(capacity);
        pool.setMaxFreeThreads(0);
        pool.setQueueSize(capacity);
    }

    // Accepts jobs into the queue but runs none of them, so a spawned worker stays submitted
    // without ever entering `worker()`.
    void submitWithoutStarting(size_t queue_capacity)
    {
        pool.setMaxThreads(0);
        pool.setMaxFreeThreads(0);
        pool.setQueueSize(queue_capacity);
    }

    void restore()
    {
        pool.setMaxThreads(10000);
        pool.setMaxFreeThreads(1000);
        pool.setQueueSize(10000);
    }

    ~GlobalThreadPoolLimits() { restore(); }
};

// `spawn()` blocks fault injections unless the pool has a running worker, so at probability 1 only the
// spawn made alongside a running worker fails. Which tier was taken is therefore observable in
// `AsyncLoaderSpawnFailures` without exhausting anything and without aborting.
struct AlwaysFailToAllocateThread
{
    AlwaysFailToAllocateThread() { CannotAllocateThreadFaultInjector::setFaultProbability(1.0); }
    ~AlwaysFailToAllocateThread() { CannotAllocateThreadFaultInjector::setFaultProbability(0.0); }
};

// A spawn dropped while a worker is still running costs concurrency only, so every job must still
// complete, and the pool must be probed once per saturation episode, not once per queued job.
TEST(AsyncLoader, SpawnFailureWithRunningWorkerDoesNotTerminate)
{
    GlobalThreadPoolLimits limits;
    const auto failures_before = spawnFailures();

    AsyncLoaderTest t(16); // > 1 so that every enqueue attempts a spawn
    std::barrier<std::__empty_completion> sync(2);
    std::atomic<size_t> jobs_done{0};

    auto blocking_job_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        sync.arrive_and_wait(); // (A) hold this worker running while the global pool is saturated
        jobs_done++;
    };
    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &) { jobs_done++; };

    auto blocking_job = makeLoadJob({}, "blocking_job", blocking_job_func);
    auto blocking_task = t.schedule({blocking_job});
    t.loader.unpause();

    // The first worker must be running, and not suspended, before the pool is saturated.
    while (blocking_job->startTime() == LoadJob::TimePoint{})
        std::this_thread::yield();
    limits.saturate(1);

    LoadTaskPtrs tasks;
    tasks.reserve(64);
    for (size_t i = 0; i < 64; i++)
        tasks.push_back(t.schedule({makeLoadJob({}, "job", job_func)}));

    sync.arrive_and_wait(); // (A) release the worker so it drains the queue itself
    waitLoad(blocking_task);
    waitLoad(tasks);

    ASSERT_EQ(jobs_done, 65);
    // One failed attempt per saturation episode, whatever the number of queued jobs.
    ASSERT_EQ(spawnFailures() - failures_before, 1);
    t.loader.wait();
}

// A dropped spawn must not leave `Pool::workers` overcounted: a leaked increment makes `hasWorker()`
// report a worker forever, so `wait()` never returns. Once the limits are restored the pool must
// spawn again, which is asserted by requiring two jobs to run at the same time.
TEST(AsyncLoader, SpawnFailureThenRecovers)
{
    GlobalThreadPoolLimits limits;

    AsyncLoaderTest t(16);
    std::barrier<std::__empty_completion> sync(2);
    std::atomic<size_t> jobs_done{0};

    auto blocking_job_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        sync.arrive_and_wait(); // (A)
        jobs_done++;
    };
    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &) { jobs_done++; };

    auto blocking_job = makeLoadJob({}, "blocking_job", blocking_job_func);
    auto blocking_task = t.schedule({blocking_job});
    t.loader.unpause();
    while (blocking_job->startTime() == LoadJob::TimePoint{})
        std::this_thread::yield();
    limits.saturate(1);

    LoadTaskPtrs tasks;
    tasks.reserve(32);
    for (size_t i = 0; i < 32; i++)
        tasks.push_back(t.schedule({makeLoadJob({}, "job", job_func)}));

    sync.arrive_and_wait(); // (A)
    waitLoad(blocking_task);
    waitLoad(tasks);
    ASSERT_EQ(jobs_done, 33);

    limits.restore();

    // The second job is enqueued while the first one is already running, which is the case in which
    // a spawn is droppable. It can therefore only run if the pool spawns a worker again. Both jobs
    // wait for the other to arrive, so a pool that stopped spawning leaves them both waiting; the
    // wait is bounded so that this fails the assertion instead of hanging the test binary.
    std::mutex overlap_mutex;
    std::condition_variable overlap_cv;
    size_t arrived = 0;
    size_t observed_overlap = 0;
    auto overlapping_job_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        std::unique_lock lock{overlap_mutex};
        arrived++;
        overlap_cv.notify_all();
        if (overlap_cv.wait_for(lock, std::chrono::seconds(30), [&] { return arrived == 2; }))
            observed_overlap++;
    };

    auto first_job = makeLoadJob({}, "overlapping_job0", overlapping_job_func);
    auto first_overlapping_task = t.schedule({first_job});
    while (first_job->startTime() == LoadJob::TimePoint{})
        std::this_thread::yield();
    auto second_overlapping_task = t.schedule({makeLoadJob({}, "overlapping_job1", overlapping_job_func)});
    waitLoad(first_overlapping_task);
    waitLoad(second_overlapping_task);

    ASSERT_EQ(observed_overlap, 2);
    t.loader.wait();
}

// The reported abort happens on the job-completion path: a worker calls `finish()`, which enqueues
// the now-ready dependent job and spawns a worker for it. The calling worker is still running, so
// dropping that spawn is safe and it picks the dependent job up itself.
TEST(AsyncLoader, SpawnFailureFromFinishDoesNotTerminate)
{
    GlobalThreadPoolLimits limits;

    AsyncLoaderTest t(16);
    std::barrier<std::__empty_completion> sync(2);
    std::atomic<size_t> jobs_done{0};

    auto blocking_job_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        sync.arrive_and_wait(); // (A) saturate the global pool while this worker runs
        jobs_done++;
    };
    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &) { jobs_done++; };

    // Dependents are enqueued from `finish()`, i.e. from inside the worker, which is the reported path.
    auto blocking_job = makeLoadJob({}, "blocking_job", blocking_job_func);
    std::vector<LoadJobPtr> jobs{blocking_job};
    for (size_t i = 0; i < 8; i++)
        jobs.push_back(makeLoadJob({blocking_job}, fmt::format("dependent_job{}", i), job_func));
    auto task = t.schedule({jobs.begin(), jobs.end()});

    t.loader.unpause();
    while (blocking_job->startTime() == LoadJob::TimePoint{}) // The worker must be running before saturating
        std::this_thread::yield();
    limits.saturate(1);

    const auto failures_before = spawnFailures();
    sync.arrive_and_wait(); // (A) `finish(blocking_job)` now enqueues the dependents and fails to spawn
    waitLoad(task);

    ASSERT_EQ(jobs_done, 9);
    // Completion alone would also hold if the spawn were skipped, so require that it was attempted.
    ASSERT_EQ(spawnFailures() - failures_before, 1);
    t.loader.wait();
}

// A submitted but not yet started worker cannot take a job from the ready queue, so a pool holding
// only such workers still needs a spawn. Measurements are taken into locals and asserted only after
// the limits are restored: an early return while the pool cannot run jobs hangs in `~LoadTask`.
TEST(AsyncLoader, SpawnIsRequiredWhileNoWorkerHasStarted)
{
    GlobalThreadPoolLimits limits;

    AsyncLoaderTest t(16);
    std::atomic<size_t> jobs_done{0};
    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &) { jobs_done++; };

    t.loader.unpause();
    limits.submitWithoutStarting(1000); // Loader workers get queued in the global pool but never start

    LoadTaskPtrs tasks;
    tasks.reserve(8);
    for (size_t i = 0; i < 4; i++)
        tasks.push_back(t.schedule({makeLoadJob({}, "job", job_func)}));

    // No worker has started, so every spawn here runs with injections blocked and succeeds. One that
    // counted a queued worker as a drainer would call `trySchedule`, which the injector fails.
    const auto failures_before = spawnFailures();
    const auto submitted_before = limits.pool.active();
    {
        AlwaysFailToAllocateThread always_fail;
        for (size_t i = 0; i < 4; i++)
            tasks.push_back(t.schedule({makeLoadJob({}, "job", job_func)}));
    }
    const auto failures = spawnFailures() - failures_before;
    // Zero failures alone would also hold if no spawn had been attempted, so require that each of the
    // four enqueues submitted a worker of its own.
    const auto submitted = limits.pool.active() - submitted_before;

    limits.restore(); // Let the submitted workers start and drain everything
    waitLoad(tasks);

    ASSERT_EQ(failures, 0);
    ASSERT_EQ(submitted, 4);
    ASSERT_EQ(jobs_done, 8);
    t.loader.wait();
}

// A worker blocked in `wait()` cannot take a job from its own pool's ready queue even when it waits
// for a job of another pool, a case that priority inheritance does not convert into a same-pool wait.
// Treating it as a drainer leaves that pool's queue with nobody to run it.
TEST(AsyncLoader, SpawnIsAttemptedWhileTheOnlyWorkerWaitsForAnotherPool)
{
    // Sibling pools of equal priority: `prioritize()` refuses to move a job between them and `wait()`
    // does not inherit priority, so the wait stays cross-pool and is never counted as suspended.
    // Equal priority is also what lets both pools spawn at the same time.
    AsyncLoaderTest t({
        {.max_threads = 2, .priority = Priority{0}},
        {.max_threads = 2, .priority = Priority{0}},
    });

    std::barrier<std::__empty_completion> sync(2);
    std::atomic<size_t> jobs_done{0};
    auto job_func = [&] (AsyncLoader &, const LoadJobPtr &) { jobs_done++; };

    // Held pending until the measurement is taken, so the waiting worker really is inside `wait()`.
    auto awaited_job_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        sync.arrive_and_wait(); // (A)
        jobs_done++;
    };
    auto awaited_job = makeLoadJob({}, 0, "awaited_job", awaited_job_func);
    auto awaited_task = t.schedule({awaited_job});

    auto waiting_job_func = [&] (AsyncLoader & loader, const LoadJobPtr &)
    {
        loader.wait(awaited_job); // Blocks this pool-1 worker on a pool-0 job
        jobs_done++;
    };
    auto waiting_job = makeLoadJob({}, 1, "waiting_job", waiting_job_func);
    auto waiting_task = t.schedule({waiting_job});
    t.loader.unpause();

    while (awaited_job->waitersCount() == 0) // Pool 1's only worker is inside `wait()`
        std::this_thread::yield();
    const auto suspended_in_pool1 = t.loader.suspendedWorkersCount(1);

    // Queue a job in pool 1 while its only worker is inside `wait()`. That spawn runs with injections
    // blocked, so it succeeds; treating the waiter as a drainer would leave the job with nobody to run it.
    const auto failures_before = spawnFailures();
    auto queued_job = makeLoadJob({}, 1, "queued_job", job_func);
    LoadTaskPtr queued_task;
    {
        AlwaysFailToAllocateThread always_fail;
        queued_task = t.schedule({queued_job});
    }
    const auto failures = spawnFailures() - failures_before;

    // The spawned worker must run the queued job while the wait is still outstanding. Zero failures
    // alone would also hold if no spawn had been attempted, since the waiter drains once released.
    // Polled rather than awaited, so a queue left with no worker fails here instead of hanging.
    bool ran_during_wait = false;
    for (size_t i = 0; i < 3000 && !ran_during_wait; i++)
    {
        ran_during_wait = queued_job->status() == LoadStatus::OK;
        if (!ran_during_wait)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    sync.arrive_and_wait(); // (A) release the awaited job before asserting, so a failure cannot hang
    waitLoad(awaited_task);
    waitLoad(waiting_task);
    waitLoad(queued_task);

    ASSERT_EQ(suspended_in_pool1, 0); // The cross-pool wait is not counted as suspended
    ASSERT_EQ(failures, 0);
    ASSERT_TRUE(ran_during_wait);
    ASSERT_EQ(jobs_done, 3);
    t.loader.wait();
}

// A job left in a pool's ready queue is drained by that pool's running worker, so no spawn is needed
// while such a worker exists. When that worker enters `wait()` it stops draining, and the job needs a
// worker of its own from that moment, whichever pool the awaited job belongs to. A wait that does not
// reconsider spawning leaves the job queued for the whole duration of the wait.
TEST(AsyncLoader, SpawnIsAttemptedWhenTheOnlyWorkerStartsWaitingForAnotherPool)
{
    // Sibling pools of equal priority, so `prioritize()` refuses to move a job between them and
    // `wait()` does not inherit priority: the wait stays cross-pool.
    AsyncLoaderTest t({
        {.max_threads = 2, .priority = Priority{0}},
        {.max_threads = 2, .priority = Priority{0}},
    });

    std::barrier<std::__empty_completion> sync(2);
    std::atomic<bool> start_waiting{false};
    std::atomic<bool> queued_job_done{false};
    std::atomic<size_t> jobs_done{0};

    // Held pending until the measurement is taken, so the cross-pool wait is still outstanding then.
    auto awaited_job_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        sync.arrive_and_wait(); // (A)
        jobs_done++;
    };
    auto awaited_job = makeLoadJob({}, 0, "awaited_job", awaited_job_func);
    auto awaited_task = t.schedule({awaited_job});

    auto waiting_job_func = [&] (AsyncLoader & loader, const LoadJobPtr &)
    {
        while (!start_waiting.load()) // Stay running, and not waiting, while the next job is queued
            std::this_thread::yield();
        loader.wait(awaited_job); // Blocks this pool-1 worker on a pool-0 job
        jobs_done++;
    };
    auto waiting_job = makeLoadJob({}, 1, "waiting_job", waiting_job_func);
    auto waiting_task = t.schedule({waiting_job});
    t.loader.unpause();

    while (waiting_job->startTime() == LoadJob::TimePoint{})
        std::this_thread::yield();

    // Queue a job in pool 1 while that pool has a running worker to drain it. The spawn is optional
    // here, and the injector makes it fail, which is the state the pool is meant to tolerate.
    LoadTaskPtr queued_task;
    {
        AlwaysFailToAllocateThread always_fail;
        queued_task = t.schedule({makeLoadJob({}, 1, "queued_job", [&] (AsyncLoader &, const LoadJobPtr &)
        {
            queued_job_done = true;
            jobs_done++;
        })});
    }

    // The only worker able to run it now enters a cross-pool wait.
    start_waiting = true;
    while (awaited_job->waitersCount() == 0)
        std::this_thread::yield();

    // The queued job must run while that wait is still outstanding. Bounded, so a pool that never
    // spawns fails by assertion instead of hanging here.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!queued_job_done.load() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    const bool ran_during_wait = queued_job_done.load();

    sync.arrive_and_wait(); // (A) release the awaited job, so everything drains either way
    waitLoad(awaited_task);
    waitLoad(waiting_task);
    waitLoad(queued_task);

    ASSERT_TRUE(ran_during_wait);
    ASSERT_EQ(jobs_done, 3);
    t.loader.wait();
}

// A worker waiting on another pool's job resumes draining its own queue once that job runs, so a
// failed spawn costs latency, not liveness, and must not be fatal. Measurements are taken into
// locals and asserted only after the limits are restored: an early return hangs in `~LoadTask`.
TEST(AsyncLoader, SpawnFailureWithOnlyAWorkerWaitingForAnotherPoolDoesNotTerminate)
{
    GlobalThreadPoolLimits limits;

    // Sibling pools of equal priority, so `prioritize()` refuses to move a job between them and
    // `wait()` does not inherit priority: the wait stays cross-pool and is never counted as suspended.
    AsyncLoaderTest t({
        {.max_threads = 2, .priority = Priority{0}},
        {.max_threads = 2, .priority = Priority{0}},
    });

    std::barrier<std::__empty_completion> sync(2);
    std::atomic<bool> start_waiting{false};
    std::atomic<size_t> jobs_done{0};

    // Held pending until the measurement is taken, so the cross-pool wait is still outstanding then.
    auto awaited_job_func = [&] (AsyncLoader &, const LoadJobPtr &)
    {
        sync.arrive_and_wait(); // (A)
        jobs_done++;
    };
    auto awaited_job = makeLoadJob({}, 0, "awaited_job", awaited_job_func);
    auto awaited_task = t.schedule({awaited_job});

    auto waiting_job_func = [&] (AsyncLoader & loader, const LoadJobPtr &)
    {
        while (!start_waiting.load()) // Stay running while the global pool is exhausted and a job is queued
            std::this_thread::yield();
        loader.wait(awaited_job); // Blocks this pool-1 worker on a pool-0 job, and spawns from there
        jobs_done++;
    };
    auto waiting_job = makeLoadJob({}, 1, "waiting_job", waiting_job_func);
    auto waiting_task = t.schedule({waiting_job});
    t.loader.unpause();

    // Both workers must be running before the global pool is exhausted, so that the spawn attempted
    // from inside `wait()` is the one that fails.
    while (awaited_job->startTime() == LoadJob::TimePoint{} || waiting_job->startTime() == LoadJob::TimePoint{})
        std::this_thread::yield();
    limits.saturate(2);

    const auto failures_before = spawnFailures();

    // Queued while pool 1 still has a running worker to drain it, so this spawn is droppable and its
    // failure is expected. It also arms the memo, which must not suppress the attempt made below.
    auto queued_task = t.schedule({makeLoadJob({}, 1, "queued_job", [&] (AsyncLoader &, const LoadJobPtr &) { jobs_done++; })});
    const auto failures_after_queueing = spawnFailures() - failures_before;

    // Pool 1's only worker now enters a cross-pool wait, which reaches the spawn while the global pool
    // still cannot provide a thread. Reaching a fatal branch here would abort the whole test binary.
    start_waiting = true;
    while (awaited_job->waitersCount() == 0)
        std::this_thread::yield();
    const auto failures_after_wait = spawnFailures() - failures_before;

    sync.arrive_and_wait(); // (A) release the awaited job, so the wait ends whatever happened
    limits.restore();
    waitLoad(awaited_task);
    waitLoad(waiting_task);
    waitLoad(queued_task);

    // Surviving is necessary but not sufficient: the spawn attempted from inside `wait()` must have
    // been made and must have failed, otherwise this test would pass without exercising the branch.
    ASSERT_EQ(failures_after_queueing, 1); // The droppable spawn, before the wait
    ASSERT_EQ(failures_after_wait, 2); // Plus the one attempted from inside `wait()`
    ASSERT_EQ(jobs_done, 3); // The queued job still runs once the wait ends
    t.loader.wait();
}

// The abort is kept for the one state that cannot be recovered from: a pool with no worker able to
// drain its queue, and no thread to be had. Dropping the spawn there would leave the jobs queued
// forever, so a failure to spawn must remain fatal.
TEST(AsyncLoaderDeathTest, MandatorySpawnFailureTerminates)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    auto mandatory_spawn_with_no_thread_available = []
    {
        // Fill the global thread pool's queue while it can run nothing, so the next request for a
        // thread fails immediately rather than waiting. Only this child process is affected.
        GlobalThreadPoolLimits limits;
        limits.submitWithoutStarting(1);
        GlobalThreadPool::instance().scheduleOrThrow([] {}, {}, /* wait_microseconds = */ 0);

        // A pool whose workers have all yet to start needs this spawn to succeed, so the fault
        // injector is bypassed and the failure cannot be dropped.
        AsyncLoaderTest t(16);
        auto task = t.schedule({makeLoadJob({}, "job", [] (AsyncLoader &, const LoadJobPtr &) {})});
        t.loader.unpause(); // Spawns, and must not return

        // Reached only if the failure was swallowed. Exiting successfully reports that to the parent,
        // whereas draining would be impossible here and would hang instead.
        std::_Exit(0);
    };

    EXPECT_DEATH(mandatory_spawn_with_no_thread_available(), "Cannot schedule a task");
}
