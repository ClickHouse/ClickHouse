#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>
#include <base/scope_guard.h>

#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <functional>
#include <future>
#include <latch>
#include <memory>
#include <random>
#include <thread>


using namespace DB;

namespace CurrentMetrics
{
    extern const Metric BackgroundMergesAndMutationsPoolTask;
    extern const Metric BackgroundMergesAndMutationsPoolSize;
}

namespace ProfileEvents
{
    extern const Event CommonBackgroundExecutorTaskExecuteStepMicroseconds;
    extern const Event CommonBackgroundExecutorTaskCancelMicroseconds;
    extern const Event CommonBackgroundExecutorTaskResetMicroseconds;
    extern const Event CommonBackgroundExecutorWaitMicroseconds;
}

namespace DB::FailPoints
{
    extern const char merge_tree_background_task_marked_for_deletion[];
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

    void cancel() noexcept override { /* no op */ }

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

    void cancel() noexcept override { chassert(false, "Not implemented"); }

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
        ThreadName::TEST_SCHEDULER,
        1, // threads
        100, // max_tasks
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask,
        CurrentMetrics::BackgroundMergesAndMutationsPoolSize,
        ProfileEvents::CommonBackgroundExecutorTaskExecuteStepMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskCancelMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskResetMicroseconds,
        ProfileEvents::CommonBackgroundExecutorWaitMicroseconds
    );

    String schedule; // mutex is not required because we have a single worker
    String expected_schedule = "ABCDEABCDABCDBCDCDD";
    std::barrier<std::__empty_completion> barrier(2);
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

TEST(Executor, ReserveTaskSlotsWaitsForAvailableSlot)
{
    auto executor = std::make_shared<DB::MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue>>
    (
        ThreadName::TEST_SCHEDULER,
        1, // threads
        1, // max_tasks
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask,
        CurrentMetrics::BackgroundMergesAndMutationsPoolSize,
        ProfileEvents::CommonBackgroundExecutorTaskExecuteStepMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskCancelMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskResetMicroseconds,
        ProfileEvents::CommonBackgroundExecutorWaitMicroseconds
    );

    ASSERT_EQ(executor->tryReserveTaskSlots(1), 1);

    auto reserved_slot = std::async(std::launch::async, [&]
    {
        return executor->reserveTaskSlots(1);
    });

    ASSERT_EQ(reserved_slot.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    executor->releaseTaskSlots(1);
    ASSERT_EQ(reserved_slot.get(), 1);
    executor->releaseTaskSlots(1);
    executor->wait();
}


TEST(Executor, RemoveTasks)
{
    const size_t tasks_kinds = 25;
    const size_t batch = 100;

    auto executor = std::make_shared<DB::MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue>>
    (
        ThreadName::TEST_SCHEDULER,
        tasks_kinds,
        tasks_kinds * batch,
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask,
        CurrentMetrics::BackgroundMergesAndMutationsPoolSize,
        ProfileEvents::CommonBackgroundExecutorTaskExecuteStepMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskCancelMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskResetMicroseconds,
        ProfileEvents::CommonBackgroundExecutorWaitMicroseconds
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
        ThreadName::TEST_SCHEDULER,
        tasks_kinds,
        tasks_kinds * batch * (schedulers_count + removers_count),
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask,
        CurrentMetrics::BackgroundMergesAndMutationsPoolSize,
        ProfileEvents::CommonBackgroundExecutorTaskExecuteStepMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskCancelMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskResetMicroseconds,
        ProfileEvents::CommonBackgroundExecutorWaitMicroseconds
    );

    std::barrier<std::__empty_completion> barrier(schedulers_count + removers_count);

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
        ThreadName::TEST_SCHEDULER,
        1, // threads
        100, // max_tasks
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask,
        CurrentMetrics::BackgroundMergesAndMutationsPoolSize,
        ProfileEvents::CommonBackgroundExecutorTaskExecuteStepMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskCancelMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskResetMicroseconds,
        ProfileEvents::CommonBackgroundExecutorWaitMicroseconds
    );

    String schedule; // mutex is not required because we have a single worker
    String expected_schedule = "ABCDEDDDDDCCBACBACB";
    std::barrier<std::__empty_completion> barrier(2);
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


/// Task whose destructor is artificially slow, simulating expensive cleanup
/// (e.g. finalizing write buffers, releasing table locks).
class SlowDestructorTask : public IExecutableTask
{
public:
    SlowDestructorTask(const String & name_, std::latch & destruction_started_, std::chrono::milliseconds delay_)
        : name(name_)
        , destruction_started(destruction_started_)
        , delay(delay_)
    {}

    ~SlowDestructorTask() override
    {
        destruction_started.count_down();
        std::this_thread::sleep_for(delay);
    }

    bool executeStep() override { return false; }
    void cancel() noexcept override {}
    StorageID getStorageID() const override { return {"test", name}; }
    void onCompleted() override {}
    Priority getPriority() const override { return {}; }
    String getQueryId() const override { return "test::slow_destructor"; }

private:
    String name;
    std::latch & destruction_started;
    std::chrono::milliseconds delay;
};


/// Demonstrates that slow task destruction blocks `removeTasksCorrespondingToStorage`
/// because `resetTask` is called while holding the executor's mutex.
///
/// The test schedules a task with a slow destructor for storage "slow_storage",
/// waits for destruction to begin (meaning the mutex is held), then calls
/// `removeTasksCorrespondingToStorage` for a completely UNRELATED storage.
/// That call should complete instantly but instead gets blocked for the
/// entire duration of the task destructor.
TEST(Executor, SlowTaskDestructionBlocksRemoveTasks)
{
    static constexpr int DESTRUCTION_DELAY_MS = 500;

    /// Declared before executor so it outlives it (executor's destructor calls `wait`).
    std::latch destruction_started(1);

    auto executor = std::make_shared<MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue>>
    (
        ThreadName::TEST_SCHEDULER,
        1, // threads
        10, // max_tasks
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask,
        CurrentMetrics::BackgroundMergesAndMutationsPoolSize,
        ProfileEvents::CommonBackgroundExecutorTaskExecuteStepMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskCancelMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskResetMicroseconds,
        ProfileEvents::CommonBackgroundExecutorWaitMicroseconds
    );

    executor->trySchedule(std::make_shared<SlowDestructorTask>(
        "slow_storage", destruction_started, std::chrono::milliseconds(DESTRUCTION_DELAY_MS)));

    /// Wait until the worker thread enters the task's destructor,
    /// which means the executor's mutex is held by `release_task`.
    destruction_started.wait();

    /// Call `removeTasksCorrespondingToStorage` for a completely unrelated storage.
    /// This needs the mutex only briefly (scan queues, find nothing, return),
    /// but will be blocked for the entire duration of the slow destructor.
    Stopwatch watch;
    executor->removeTasksCorrespondingToStorage({"test", "unrelated_storage"});
    auto elapsed_ms = watch.elapsedMilliseconds();

    /// After fixing the issue (moving `resetTask` outside the lock),
    /// this should complete in under 50 ms. Currently it takes ~500 ms.
    EXPECT_LT(elapsed_ms, 100)
        << "removeTasksCorrespondingToStorage for an unrelated storage was blocked for "
        << elapsed_ms << " ms by slow task destruction holding the mutex";

    executor->wait();
}


/// Task that always asks for another step, so `routine` keeps re-pushing it to the pending queue.
/// It records whether it was cancelled; a real merge task releases most of its resources in
/// `cancel` and keeps its `TableLockHolder` alive for as long as the task object exists.
class NeverFinishingTask : public IExecutableTask
{
public:
    NeverFinishingTask(const String & name_, std::latch & step_executed_, std::atomic<bool> & cancelled_)
        : name(name_)
        , step_executed(step_executed_)
        , cancelled(cancelled_)
    {}

    bool executeStep() override
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        if (!step_reported.exchange(true))
            step_executed.count_down();
        return true;
    }

    void cancel() noexcept override { cancelled = true; }
    StorageID getStorageID() const override { return {"test", name}; }
    void onCompleted() override {}
    Priority getPriority() const override { return {}; }
    String getQueryId() const override { return "test::never_finishing"; }

private:
    String name;
    std::latch & step_executed;
    std::atomic<bool> & cancelled;
    std::atomic<bool> step_reported{false};
};


/// A multi-step task waiting in the pending queue must not survive `wait()`.
///
/// `threadFunction` breaks out of its loop before popping once `shutdown` is set, so a task that
/// `routine` re-pushed for another step is neither resumed nor cancelled. A real merge task holds
/// a `TableLockHolder` (a table share lock) for its whole lifetime, so such an abandoned task
/// blocks every later exclusive lock on its table until the acquisition times out. That is how a
/// `RENAME` rotating an obsolete `system.session_log` during shutdown ends in `DEADLOCK_AVOIDED`
/// once that timeout expires.
TEST(Executor, ShutdownReleasesPendingTasks)
{
    /// More than one task, so that draining a single one is not enough to pass.
    static constexpr size_t tasks_count = 3;

    /// Declared before the executor so they outlive it (its destructor calls `wait`).
    std::latch step_executed(tasks_count);
    std::array<std::atomic<bool>, tasks_count> cancelled{};
    std::array<std::weak_ptr<IExecutableTask>, tasks_count> weak_tasks;

    auto executor = std::make_shared<MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue>>
    (
        ThreadName::TEST_SCHEDULER,
        1, // threads
        10, // max_tasks
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask,
        CurrentMetrics::BackgroundMergesAndMutationsPoolSize,
        ProfileEvents::CommonBackgroundExecutorTaskExecuteStepMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskCancelMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskResetMicroseconds,
        ProfileEvents::CommonBackgroundExecutorWaitMicroseconds
    );

    for (size_t i = 0; i < tasks_count; ++i)
    {
        auto task = std::make_shared<NeverFinishingTask>(
            "pending_storage_" + std::to_string(i), step_executed, cancelled[i]);
        weak_tasks[i] = task;
        ASSERT_TRUE(executor->trySchedule(std::move(task)));
    }

    /// Wait until every task has run a step, so each is known to be cycling through the pending
    /// queue rather than never having been picked up. The single worker pops them in turn, and
    /// `routine` re-pushes each one, so all of them are queued again by the time `wait()` runs.
    step_executed.wait();

    executor->wait();

    for (size_t i = 0; i < tasks_count; ++i)
    {
        EXPECT_TRUE(weak_tasks[i].expired())
            << "task " << i << " was left in the pending queue and outlived executor shutdown, so "
               "the resources it owns (for a merge task, a table share lock) were never released";
        EXPECT_TRUE(cancelled[i].load())
            << "task " << i << " was left in the pending queue and not cancelled on executor shutdown";
    }
}


/// Task that keeps asking for another step and whose destructor blocks until released once it has run a step,
/// so the shutdown drain keeps it alive for as long as the test needs.
class BlockingDestructionTask : public IExecutableTask
{
public:
    BlockingDestructionTask(
        const String & name_,
        std::latch & step_executed_,
        std::latch & destruction_started_,
        std::shared_future<void> destruction_released_,
        std::atomic<bool> & destroyed_)
        : name(name_)
        , step_executed(step_executed_)
        , destruction_started(destruction_started_)
        , destruction_released(std::move(destruction_released_))
        , destroyed(destroyed_)
    {}

    ~BlockingDestructionTask() override
    {
        /// Not blocking a task that never ran: `trySchedule` failing must fail the test, not hang it.
        if (step_reported)
        {
            destruction_started.count_down();
            destruction_released.wait();
        }
        destroyed = true;
    }

    bool executeStep() override
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        if (!step_reported.exchange(true))
            step_executed.count_down();
        return true;
    }

    void cancel() noexcept override {}
    StorageID getStorageID() const override { return {"test", name}; }
    void onCompleted() override {}
    Priority getPriority() const override { return {}; }
    String getQueryId() const override { return "test::blocking_destruction"; }

private:
    String name;
    std::latch & step_executed;
    std::latch & destruction_started;
    std::shared_future<void> destruction_released;
    std::atomic<bool> & destroyed;
    std::atomic<bool> step_reported{false};
};


/// A task that `wait()` is still destroying must keep `removeTasksCorrespondingToStorage` waiting.
/// Otherwise a storage tearing down its background work during shutdown (a `ReplicatedMergeTree` restarting thread on
/// a ZooKeeper session expiry) re-initializes its replication queue while a merge task still holds its future parts.
TEST(Executor, RemoveTasksWaitsForTasksDrainedOnShutdown)
{
    /// Declared before the executor so they outlive it (its destructor calls `wait`).
    std::latch step_executed(1);
    std::latch destruction_started(1);
    std::promise<void> destruction_release;
    std::promise<void> remove_returned;
    std::atomic<bool> destroyed{false};
    std::atomic<bool> destroyed_when_returned{false};

    /// `removeTasksCorrespondingToStorage` pauses at this fail point once it has found a task it must wait for.
    /// Enabled before any task exists: if this throws, unwinding must not reach a task blocked in its destructor.
    const String fail_point = FailPoints::merge_tree_background_task_marked_for_deletion;
    FailPointInjection::enableFailPoint(fail_point);
    SCOPE_EXIT({ FailPointInjection::disableFailPoint(fail_point); });

    auto executor = std::make_shared<MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue>>
    (
        ThreadName::TEST_SCHEDULER,
        1, // threads
        10, // max_tasks
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask,
        CurrentMetrics::BackgroundMergesAndMutationsPoolSize,
        ProfileEvents::CommonBackgroundExecutorTaskExecuteStepMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskCancelMicroseconds,
        ProfileEvents::CommonBackgroundExecutorTaskResetMicroseconds,
        ProfileEvents::CommonBackgroundExecutorWaitMicroseconds
    );

    ASSERT_TRUE(executor->trySchedule(std::make_shared<BlockingDestructionTask>(
        "drained_storage", step_executed, destruction_started, destruction_release.get_future().share(), destroyed)));

    /// The task is now cycling through `pending`, so `wait()` finds it there.
    step_executed.wait();

    std::thread shutdown_thread([&] { executor->wait(); });

    /// The drain is inside the task's destructor: the task is alive and owned by `wait()`.
    destruction_started.wait();

    auto remove_returned_future = remove_returned.get_future();
    std::thread remove_thread([&]
    {
        executor->removeTasksCorrespondingToStorage({"test", "drained_storage"});
        destroyed_when_returned = destroyed.load();
        remove_returned.set_value();
        /// Wakes `waitForPause` below if the call returned without pausing.
        FailPointInjection::disableFailPoint(fail_point);
    });

    /// Returns once the call has either paused before waiting for the task, or returned.
    FailPointInjection::waitForPause(fail_point);
    FailPointInjection::disableFailPoint(fail_point);

    /// The task is still being destroyed, so the call must still be waiting for it.
    EXPECT_EQ(remove_returned_future.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout)
        << "removeTasksCorrespondingToStorage returned while the shutdown drain was still destroying a task of that storage";

    destruction_release.set_value();

    remove_thread.join();
    shutdown_thread.join();

    EXPECT_TRUE(destroyed_when_returned.load())
        << "removeTasksCorrespondingToStorage returned while a task of that storage was still alive in the shutdown drain";
    EXPECT_TRUE(destroyed.load());
}
