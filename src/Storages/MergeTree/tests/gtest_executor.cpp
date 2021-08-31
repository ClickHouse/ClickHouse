#include <gtest/gtest.h>

#include <atomic>
#include <memory>

#include <Storages/MergeTree/ExecutableTask.h>
#include <Storages/MergeTree/MergeMutateExecutor.h>

using namespace DB;

namespace CurrentMetrics
{
    extern const Metric BackgroundPoolTask;
}

class FakeExecutableTask : public ExecutableTask
{
public:
    explicit FakeExecutableTask(String name_, std::function<void()> on_completed_) : name(name_), on_completed(on_completed_)
    {
    }

    bool execute() override
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        return false;
    }

    StorageID getStorageID() override
    {
        return {"test", name};
    }

    void onCompleted() override
    {
        on_completed();
    }

private:

    String name;
    std::function<void()> on_completed;
};


TEST(Executor, Simple)
{
    auto executor = DB::MergeTreeBackgroundExecutor::create();

    const size_t tasks_kinds = 25;
    const size_t batch = 100;

    executor->setThreadsCount([]() { return 25; });
    executor->setTasksCount([] () { return tasks_kinds * batch; });
    executor->setMetric(CurrentMetrics::BackgroundPoolTask);

    for (size_t i = 0; i < 4; ++i)
    {
        for (size_t j = 0; j < tasks_kinds; ++j)
        {
            bool res = executor->trySchedule(std::make_shared<FakeExecutableTask>(std::to_string(j), [](){}));
            ASSERT_TRUE(res);
        }
    }

    std::vector<std::thread> threads(batch);

    for (auto & thread : threads)
        thread = std::thread([&] ()
        {
            for (size_t j = 0; j < tasks_kinds; ++j)
                executor->removeTasksCorrespondingToStorage({"test", std::to_string(j)});

        });

    for (auto & thread : threads)
        thread.join();

    ASSERT_EQ(executor->active(), 0);
    ASSERT_EQ(executor->pending(), 0);
    ASSERT_EQ(CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask], 0);

    executor->wait();

}
