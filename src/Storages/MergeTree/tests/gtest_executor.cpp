#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <memory>
#include <random>

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>

using namespace DB;

namespace CurrentMetrics
{
    extern const Metric BackgroundMergesAndMutationsPoolTask;
}

std::random_device device;

class FakeExecutableTask : public IExecutableTask
{
public:
    explicit FakeExecutableTask(String name_) : generator(device()), distribution(0, 5), name(name_)
    {
    }

    bool executeStep() override
    {
        auto sleep_time = distribution(generator);
        std::this_thread::sleep_for(std::chrono::milliseconds(5 * sleep_time));

        auto choice = distribution(generator);
        if (choice == 0)
            throw std::runtime_error("Unlucky...");

        return false;
    }

    StorageID getStorageID() override
    {
        return {"test", name};
    }

    void onCompleted() override
    {
        auto choice = distribution(generator);
        if (choice == 0)
            throw std::runtime_error("Unlucky...");
    }

    UInt64 getPriority() override { return 0; }

private:
    std::mt19937 generator;
    std::uniform_int_distribution<> distribution;

    String name;
    std::function<void()> on_completed;
};


TEST(Executor, RemoveTasks)
{
    const size_t tasks_kinds = 25;
    const size_t batch = 100;

    auto executor = DB::OrdinaryBackgroundExecutor::create
    (
        "GTest",
        tasks_kinds,
        tasks_kinds * batch,
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask
    );

    for (size_t i = 0; i < batch; ++i)
        for (size_t j = 0; j < tasks_kinds; ++j)
            ASSERT_TRUE(
                executor->trySchedule(std::make_shared<FakeExecutableTask>(std::to_string(j)))
            );

    std::vector<std::thread> threads(batch);

    auto remover_routine = [&] ()
    {
        for (size_t j = 0; j < tasks_kinds; ++j)
            executor->removeTasksCorrespondingToStorage({"test", std::to_string(j)});
    };

    for (auto & thread : threads)
        thread = std::thread(remover_routine);

    for (auto & thread : threads)
        thread.join();

    ASSERT_EQ(CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask], 0);

    executor->wait();
}


TEST(Executor, RemoveTasksStress)
{
    const size_t tasks_kinds = 25;
    const size_t batch = 100;
    const size_t schedulers_count = 5;
    const size_t removers_count = 5;

    auto executor = DB::OrdinaryBackgroundExecutor::create
    (
        "GTest",
        tasks_kinds,
        tasks_kinds * batch * (schedulers_count + removers_count),
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask
    );

    std::barrier barrier(schedulers_count + removers_count);

    auto scheduler_routine = [&] ()
    {
        barrier.arrive_and_wait();
        for (size_t i = 0; i < batch; ++i)
            for (size_t j = 0; j < tasks_kinds; ++j)
                executor->trySchedule(std::make_shared<FakeExecutableTask>(std::to_string(j)));
    };

    auto remover_routine = [&] ()
    {
        barrier.arrive_and_wait();
        for (size_t j = 0; j < tasks_kinds; ++j)
            executor->removeTasksCorrespondingToStorage({"test", std::to_string(j)});
    };

    std::vector<std::thread> schedulers(schedulers_count);
    for (auto & scheduler : schedulers)
        scheduler = std::thread(scheduler_routine);

    std::vector<std::thread> removers(removers_count);
    for (auto & remover : removers)
        remover = std::thread(remover_routine);

    for (auto & scheduler : schedulers)
        scheduler.join();

    for (auto & remover : removers)
        remover.join();

    for (size_t j = 0; j < tasks_kinds; ++j)
        executor->removeTasksCorrespondingToStorage({"test", std::to_string(j)});

    executor->wait();

    ASSERT_EQ(CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask], 0);
}
