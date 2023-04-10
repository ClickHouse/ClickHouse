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
