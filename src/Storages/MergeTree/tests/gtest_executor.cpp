#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <memory>
#include <random>
#include <functional>

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>


using namespace DB;

namespace CurrentMetrics
{
    extern const Metric BackgroundMergesAndMutationsPoolTask;
    extern const Metric BackgroundMergesAndMutationsPoolSize;
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
            throw TestException();

        return false;
    }

    StorageID getStorageID() const override
    {
        return {"test", name};
    }

    void onCompleted() override
    {
        auto choice = distribution(generator);
        if (choice == 0)
            throw TestException();
    }

    Priority getPriority() const override { return {}; }
    String getQueryId() const override { return {}; }

private:
    std::mt19937 generator;
    std::uniform_int_distribution<> distribution;

    String name;
};

using StepFunc = std::function<void(const String & name, size_t steps_left)>;

class LambdaExecutableTask : public IExecutableTask
{
public:
    explicit LambdaExecutableTask(const String & name_, size_t step_count_, StepFunc step_func_ = {}, Int64 priority_value = 0)
        : name(name_)
        , step_count(step_count_)
        , step_func(step_func_)
        , priority{priority_value}
    {}

    bool executeStep() override
    {
        if (step_func)
            step_func(name, step_count);
        return --step_count;
    }

    StorageID getStorageID() const override
    {
        return {"test", name};
    }

    void onCompleted() override {}

    Priority getPriority() const override { return priority; }
    String getQueryId() const override { return "test::lambda"; }

private:
    String name;
    size_t step_count;
    StepFunc step_func;
    Priority priority;
};


TEST(Executor, Simple)
{
    auto executor = std::make_shared<DB::MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue>>
    (
        "GTest",
        1, // threads
        100, // max_tasks
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask,
        CurrentMetrics::BackgroundMergesAndMutationsPoolSize
    );

    String schedule; // mutex is not required because we have a single worker
    String expected_schedule = "ABCDEABCDABCDBCDCDD";
    std::barrier barrier(2);
    auto task = [&] (const String & name, size_t)
    {
        schedule += name;
        if (schedule.size() == expected_schedule.size())
            barrier.arrive_and_wait();
    };

    // Schedule tasks from this `init_task` to guarantee atomicity.
    // Worker will see pending queue when we push all tasks.
    // This is required to check scheduling properties of round-robin in deterministic way.
    auto init_task = [&] (const String &, size_t)
    {
        executor->trySchedule(std::make_shared<LambdaExecutableTask>("A", 3, task));
        executor->trySchedule(std::make_shared<LambdaExecutableTask>("B", 4, task));
        executor->trySchedule(std::make_shared<LambdaExecutableTask>("C", 5, task));
        executor->trySchedule(std::make_shared<LambdaExecutableTask>("D", 6, task));
        executor->trySchedule(std::make_shared<LambdaExecutableTask>("E", 1, task));
    };

    executor->trySchedule(std::make_shared<LambdaExecutableTask>("init_task", 1, init_task));
    barrier.arrive_and_wait(); // Do not finish until tasks are done
    executor->wait();
    ASSERT_EQ(schedule, expected_schedule);
}


TEST(Executor, RemoveTasks)
{
    const size_t tasks_kinds = 25;
    const size_t batch = 100;

    auto executor = std::make_shared<DB::MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue>>
    (
        "GTest",
        tasks_kinds,
        tasks_kinds * batch,
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask,
        CurrentMetrics::BackgroundMergesAndMutationsPoolSize
    );

    for (size_t i = 0; i < batch; ++i)
        for (size_t j = 0; j < tasks_kinds; ++j)
            ASSERT_TRUE(
                executor->trySchedule(std::make_shared<FakeExecutableTask>(std::to_string(j))));

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

    auto executor = std::make_shared<DB::MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue>>
    (
        "GTest",
        tasks_kinds,
        tasks_kinds * batch * (schedulers_count + removers_count),
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask,
        CurrentMetrics::BackgroundMergesAndMutationsPoolSize
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


TEST(Executor, UpdatePolicy)
{
    auto executor = std::make_shared<DB::MergeTreeBackgroundExecutor<DynamicRuntimeQueue>>
    (
        "GTest",
        1, // threads
        100, // max_tasks
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask,
        CurrentMetrics::BackgroundMergesAndMutationsPoolSize
    );

    String schedule; // mutex is not required because we have a single worker
    String expected_schedule = "ABCDEDDDDDCCBACBACB";
    std::barrier barrier(2);
    auto task = [&] (const String & name, size_t)
    {
        schedule += name;
        if (schedule.size() == 5)
            executor->updateSchedulingPolicy(PriorityRuntimeQueue::name);
        if (schedule.size() == 12)
            executor->updateSchedulingPolicy(RoundRobinRuntimeQueue::name);
        if (schedule.size() == expected_schedule.size())
            barrier.arrive_and_wait();
    };

    // Schedule tasks from this `init_task` to guarantee atomicity.
    // Worker will see pending queue when we push all tasks.
    // This is required to check scheduling properties in a deterministic way.
    auto init_task = [&] (const String &, size_t)
    {
        executor->trySchedule(std::make_shared<LambdaExecutableTask>("A", 3, task, 5));
        executor->trySchedule(std::make_shared<LambdaExecutableTask>("B", 4, task, 4));
        executor->trySchedule(std::make_shared<LambdaExecutableTask>("C", 5, task, 3));
        executor->trySchedule(std::make_shared<LambdaExecutableTask>("D", 6, task, 2));
        executor->trySchedule(std::make_shared<LambdaExecutableTask>("E", 1, task, 1));
    };

    executor->trySchedule(std::make_shared<LambdaExecutableTask>("init_task", 1, init_task));
    barrier.arrive_and_wait(); // Do not finish until tasks are done
    executor->wait();
    ASSERT_EQ(schedule, expected_schedule);
}
