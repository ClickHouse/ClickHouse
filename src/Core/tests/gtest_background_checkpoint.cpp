#include <Core/BackgroundSchedulePool.h>
#include <Common/Exception.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>
#include <base/scope_guard.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <stdexcept>
#include <thread>

using namespace DB;
using namespace std::chrono_literals;

namespace CurrentMetrics
{
    extern const Metric BackgroundMergesAndMutationsPoolTask;
    extern const Metric BackgroundMergesAndMutationsPoolSize;
}

namespace ProfileEvents
{
    extern const Event MergeMutateBackgroundExecutorTaskExecuteStepMicroseconds;
    extern const Event MergeMutateBackgroundExecutorTaskCancelMicroseconds;
    extern const Event MergeMutateBackgroundExecutorTaskResetMicroseconds;
    extern const Event MergeMutateBackgroundExecutorWaitMicroseconds;
}

TEST(BackgroundCheckpoint, PausePreservesImmediateAndDelayedWork)
{
    auto pool = BackgroundSchedulePool::create(2, 0, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);
    SCOPE_EXIT({ pool->join(); });
    std::promise<void> immediate_done;
    std::promise<void> delayed_done;
    auto immediate_future = immediate_done.get_future();
    auto delayed_future = delayed_done.get_future();
    auto immediate = pool->createTask(StorageID::createEmpty(), "immediate", [&] { immediate_done.set_value(); });
    auto delayed = pool->createTask(StorageID::createEmpty(), "delayed", [&] { delayed_done.set_value(); });
    {
        BackgroundSchedulePool::PauseGuard outer(*pool, std::chrono::steady_clock::now() + 10s);
        {
            BackgroundSchedulePool::PauseGuard inner(*pool, std::chrono::steady_clock::now() + 10s);
            std::thread producer([&]
            {
                EXPECT_TRUE(immediate->schedule());
                EXPECT_TRUE(delayed->scheduleAfter(0));
            });
            producer.join();
        }
        outer.waitUntilIdle(std::chrono::steady_clock::now() + 10s);
        EXPECT_EQ(immediate_future.wait_for(20ms), std::future_status::timeout);
        EXPECT_EQ(delayed_future.wait_for(20ms), std::future_status::timeout);
        const auto tasks = pool->getTasks();
        ASSERT_EQ(tasks.size(), 2);
        for (const auto & task : tasks)
        {
            EXPECT_FALSE(task.executing);
            if (task.log_name == "delayed")
                EXPECT_TRUE(task.delayed);
        }
    }
    EXPECT_EQ(immediate_future.wait_for(10s), std::future_status::ready);
    EXPECT_EQ(delayed_future.wait_for(10s), std::future_status::ready);
}

TEST(BackgroundCheckpoint, PauseTimeoutResumesPool)
{
    auto pool = BackgroundSchedulePool::create(1, 1, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);
    SCOPE_EXIT({ pool->join(); });
    std::promise<void> cleanup_started;
    auto cleanup_future = cleanup_started.get_future();
    std::promise<void> release_cleanup;
    auto release_future = release_cleanup.get_future();
    std::promise<void> next_done;
    auto next_future = next_done.get_future();
    auto task = pool->createTask(StorageID::createEmpty(), "blocked_cleanup", [&]
    {
        SCOPE_EXIT({
            cleanup_started.set_value();
            release_future.wait();
        });
    });
    auto next = pool->createTask(StorageID::createEmpty(), "next", [&] { next_done.set_value(); });
    {
        SCOPE_EXIT({ release_cleanup.set_value(); });
        ASSERT_TRUE(task->schedule());
        ASSERT_EQ(cleanup_future.wait_for(10s), std::future_status::ready);
        EXPECT_THROW(
            BackgroundSchedulePool::PauseGuard(*pool, std::chrono::steady_clock::now()),
            Exception);
        ASSERT_TRUE(next->schedule());
    }
    EXPECT_EQ(next_future.wait_for(10s), std::future_status::ready);
    EXPECT_NO_THROW(BackgroundSchedulePool::PauseGuard(*pool, std::chrono::steady_clock::now() + 10s));
}

TEST(BackgroundCheckpoint, CaptureExceptionResumesPool)
{
    auto pool = BackgroundSchedulePool::create(1, 1, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);
    SCOPE_EXIT({ pool->join(); });
    std::promise<void> done;
    auto future = done.get_future();
    auto task = pool->createTask(StorageID::createEmpty(), "after_failed_capture", [&] { done.set_value(); });
    EXPECT_THROW(
        {
            BackgroundSchedulePool::PauseGuard guard(*pool, std::chrono::steady_clock::now() + 10s);
            EXPECT_TRUE(task->schedule());
            throw std::runtime_error("heap capture failed");
        },
        std::runtime_error);
    EXPECT_EQ(future.wait_for(10s), std::future_status::ready);
}

namespace
{
class CheckpointTask : public IExecutableTask
{
public:
    CheckpointTask(std::promise<void> & destroying_, std::future<void> & release_, std::atomic<size_t> & steps_)
        : destroying(destroying_), release(release_), steps(steps_)
    {
    }

    ~CheckpointTask() override
    {
        if (steps != 0)
        {
            destroying.set_value();
            release.wait();
        }
    }

    bool executeStep() override { return ++steps < 4; }
    void cancel() noexcept override {}
    StorageID getStorageID() const override { return {"test", "checkpoint"}; }
    void onCompleted() override {}
    Priority getPriority() const override { return {}; }
    String getQueryId() const override { return "checkpoint"; }

private:
    std::promise<void> & destroying;
    std::future<void> & release;
    std::atomic<size_t> & steps;
};
}

TEST(BackgroundCheckpoint, DrainWaitsForDestructionAndAllowsContinuations)
{
    std::promise<void> destroying;
    auto destroying_future = destroying.get_future();
    std::promise<void> release;
    auto release_future = release.get_future();
    std::atomic<size_t> steps = 0;
    MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue> executor(
        ThreadName::TEST_SCHEDULER, 2, 2,
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask, CurrentMetrics::BackgroundMergesAndMutationsPoolSize,
        ProfileEvents::MergeMutateBackgroundExecutorTaskExecuteStepMicroseconds,
        ProfileEvents::MergeMutateBackgroundExecutorTaskCancelMicroseconds,
        ProfileEvents::MergeMutateBackgroundExecutorTaskResetMicroseconds,
        ProfileEvents::MergeMutateBackgroundExecutorWaitMicroseconds);
    {
        SCOPE_EXIT({ release.set_value(); });
        ASSERT_TRUE(executor.trySchedule(std::make_shared<CheckpointTask>(destroying, release_future, steps)));
        ASSERT_EQ(destroying_future.wait_for(10s), std::future_status::ready);
        EXPECT_EQ(steps, 4);
        EXPECT_THROW(executor.waitUntilIdle(std::chrono::steady_clock::now()), Exception);
    }
    EXPECT_NO_THROW(executor.waitUntilIdle(std::chrono::steady_clock::now() + 10s));
    EXPECT_EQ(CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(), 0);
}
