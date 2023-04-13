#include <gtest/gtest.h>

#include <vector>
#include <thread>
#include <pcg_random.hpp>

#include <base/types.h>
#include <base/sleep.h>
#include <Common/AsyncLoader.h>
#include <Common/randomSeed.h>


using namespace DB;


namespace CurrentMetrics
{
    extern const Metric TablesLoaderThreads;
    extern const Metric TablesLoaderThreadsActive;
}

struct AsyncLoaderTest
{
    AsyncLoader loader;

    explicit AsyncLoaderTest(size_t max_threads = 1)
        : loader(CurrentMetrics::TablesLoaderThreads, CurrentMetrics::TablesLoaderThreadsActive, max_threads)
    {}
};

TEST(AsyncLoader, Smoke)
{
    AsyncLoaderTest t(2);

    static constexpr ssize_t low_priority = -1;

    std::atomic<size_t> jobs_done{0};
    std::atomic<size_t> low_priority_jobs_done{0};

    auto job_func = [&] (const LoadJob & self) {
        jobs_done++;
        if (self.priority == low_priority)
            low_priority_jobs_done++;
    };

    {
        auto job1 = makeLoadJob({}, "job1", job_func);
        auto job2 = makeLoadJob({ job1 }, "job2", job_func);
        auto task1 = t.loader.schedule({ job1, job2 });

        auto job3 = makeLoadJob({ job2 }, "job3", job_func);
        auto job4 = makeLoadJob({ job2 }, "job4", job_func);
        auto task2 = t.loader.schedule({ job3, job4 });
        auto job5 = makeLoadJob({ job3, job4 }, "job5", job_func);
        task2.merge(t.loader.schedule({ job5 }, low_priority));

        t.loader.start();

        t.loader.wait();
    }

    ASSERT_EQ(jobs_done, 5);
    ASSERT_EQ(low_priority_jobs_done, 1);

    t.loader.stop();
}

TEST(AsyncLoader, CycleDetection)
{
    AsyncLoaderTest t;

    auto job_func = [&] (const LoadJob &) {};

    LoadJobPtr cycle_breaker; // To avoid memleak we introduce with a cycle

    try
    {
        std::vector<LoadJobPtr> jobs;
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

        auto task1 = t.loader.schedule({ jobs.begin(), jobs.end()});
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
