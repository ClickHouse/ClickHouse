#include <gtest/gtest.h>
#include <Storages/MergeTree/BackgroundJobsExecutor.h>
#include <Common/CurrentMetrics.h>
#include <Common/tests/gtest_global_context.h>
#include <memory>
#include <chrono>
using namespace std::chrono_literals;
namespace CurrentMetrics
{
    extern const Metric BackgroundPoolTask;
}

using namespace DB;

static std::atomic<Int64> counter{0};

class TestJobExecutor : public IBackgroundJobExecutor
{
public:
    explicit TestJobExecutor(Context & context)
        :IBackgroundJobExecutor(
            context,
            BackgroundTaskSchedulingSettings{},
            {PoolConfig{PoolType::MERGE_MUTATE, 4, CurrentMetrics::BackgroundPoolTask}})
        {}

protected:
    String getBackgroundTaskName() const override
    {
        return "TestTask";
    }

    std::optional<JobAndPool> getBackgroundJob() override
    {
        return JobAndPool{[] { std::this_thread::sleep_for(1s); counter++; }, PoolType::MERGE_MUTATE};
    }
};

using TestExecutorPtr = std::unique_ptr<TestJobExecutor>;

TEST(BackgroundExecutor, TestMetric)
{
    const auto & context_holder = getContext();
    std::vector<TestExecutorPtr> executors;
    for (size_t i = 0; i < 100; ++i)
        executors.emplace_back(std::make_unique<TestJobExecutor>(const_cast<Context &>(context_holder.context)));

    for (size_t i = 0; i < 100; ++i)
        executors[i]->start();

    for (size_t i = 0; i < 100; ++i)
    {
        EXPECT_TRUE(CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask].load() <= 4);
        std::this_thread::sleep_for(200ms);
    }

    for (size_t i = 0; i < 100; ++i)
        executors[i]->finish();

    /// Sanity check
    EXPECT_TRUE(counter > 50);
}
