#include <Processors/Executors/Runtime/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

using namespace DB;

namespace
{

struct Fixture
{
    std::vector<ProcessorState> states;
    Poller poller;
    TaskScheduler scheduler;

    explicit Fixture(size_t workers, size_t states_count = 16)
        : states(states_count)
        , scheduler(poller, workers)
    {
    }

    Task task(size_t i, Task::Kind kind = Task::Kind::Prepare) { return Task{.state = &states[i], .kind = kind}; }

    size_t popIndex(size_t worker_id)
    {
        auto popped = scheduler.tryPop(worker_id);
        if (!popped)
            return states.size();
        return popped->state - states.data();
    }
};

}

TEST(TaskScheduler, TheLastPushedTaskRunsNextAndIsNotCounted)
{
    Fixture f(2);
    f.scheduler.push(f.task(0), 0);
    f.scheduler.push(f.task(1, Task::Kind::Work), 0);
    EXPECT_EQ(1u, f.scheduler.queued());
    EXPECT_EQ(1u, f.scheduler.total());

    auto first = f.scheduler.tryPop(0);
    ASSERT_TRUE(first);
    EXPECT_EQ(&f.states[1], first->state);
    EXPECT_EQ(Task::Kind::Work, first->kind);

    EXPECT_EQ(0u, f.popIndex(0));
    EXPECT_EQ(0u, f.scheduler.queued());
    EXPECT_FALSE(f.scheduler.tryPop(0));
}

TEST(TaskScheduler, SiblingsKeepTheirOrderThroughTheCap)
{
    Fixture f(1, 100);
    for (size_t i = 0; i < f.states.size(); ++i)
        f.scheduler.push(f.task(i), 0);

    for (size_t i = f.states.size(); i > 0; --i)
        ASSERT_EQ(i - 1, f.popIndex(0));
    EXPECT_FALSE(f.scheduler.tryPop(0));
}

TEST(TaskScheduler, LocalQueueOverflowsTheOldestHalfIntoTheGlobalQueue)
{
    Fixture f(2, 200);
    for (size_t i = 0; i < f.states.size(); ++i)
        f.scheduler.push(f.task(i), 0);
    EXPECT_EQ(f.states.size() - 1, f.scheduler.queued());

    /// The other worker takes the oldest task from the global front.
    EXPECT_EQ(0u, f.popIndex(1));
    EXPECT_EQ(199u, f.popIndex(0));
    EXPECT_EQ(1u, f.popIndex(1));
}

TEST(TaskScheduler, AChainGetsTheOldestTaskAfterTheCap)
{
    Fixture f(1, 2);
    f.scheduler.push(f.task(0), 0);

    size_t chain = 0;
    while (true)
    {
        f.scheduler.push(f.task(1), 0);
        const size_t popped = f.popIndex(0);
        ASSERT_LT(++chain, 1000u);
        if (popped == 0)
            break;
        ASSERT_EQ(1u, popped);
    }

    EXPECT_GT(chain, 1u);
    EXPECT_EQ(1u, f.popIndex(0));
    EXPECT_FALSE(f.scheduler.tryPop(0));
}

TEST(TaskScheduler, GlobalQueueIsTakenFromTheFront)
{
    Fixture f(2);
    for (size_t i = 0; i < 6; ++i)
        f.scheduler.push(f.task(i));

    /// One task at a time, the oldest first; the rest stays for others.
    EXPECT_EQ(0u, f.popIndex(1));
    EXPECT_EQ(5u, f.scheduler.queued());
    EXPECT_EQ(1u, f.popIndex(1));
    EXPECT_EQ(2u, f.popIndex(0));
    EXPECT_EQ(3u, f.popIndex(0));
    EXPECT_EQ(4u, f.popIndex(1));
    EXPECT_EQ(5u, f.popIndex(1));
    EXPECT_EQ(0u, f.scheduler.queued());
}

TEST(TaskScheduler, AnotherWorkerTakesTheOldestTask)
{
    Fixture f(2);
    for (size_t i = 0; i < 6; ++i)
        f.scheduler.push(f.task(i), 0);

    EXPECT_EQ(0u, f.popIndex(1));
    EXPECT_EQ(1u, f.popIndex(1));
    EXPECT_EQ(5u, f.popIndex(0));
    EXPECT_EQ(4u, f.popIndex(0));
    EXPECT_EQ(3u, f.popIndex(0));
    EXPECT_EQ(2u, f.popIndex(0));
    EXPECT_EQ(0u, f.scheduler.queued());
}

TEST(TaskScheduler, DrainHandsTheQueueToTheGlobalQueue)
{
    Fixture f(2);
    f.scheduler.push(f.task(0), 0);
    f.scheduler.push(f.task(1), 0);

    f.scheduler.drain(0);
    EXPECT_EQ(2u, f.scheduler.queued());
    EXPECT_EQ(0u, f.popIndex(1));
    EXPECT_EQ(1u, f.popIndex(1));
}

#if defined(OS_LINUX) || defined(OS_DARWIN)
TEST(TaskScheduler, FiredFdBecomesAnAsyncReadyTaskInTheQueue)
{
    Fixture f(1, 2);

    int fds[2];
    ASSERT_EQ(0, ::pipe(fds));
    f.scheduler.push(AsyncTask{.state = f.states.data(), .fd = fds[0], .events = EPOLLIN | EPOLLERR, .timeout_ms = -1});
    EXPECT_EQ(1u, f.poller.pending());
    EXPECT_EQ(0u, f.scheduler.queued());
    EXPECT_EQ(1u, f.scheduler.total());
    EXPECT_FALSE(f.scheduler.tryPop(0));

    f.scheduler.push(f.task(1), 0);
    EXPECT_EQ(1u, f.scheduler.total());

    /// The fired state lands in the queue, counted, and runs right after the worker's next task.
    char byte = 0;
    ASSERT_EQ(1, ::write(fds[1], &byte, 1));
    EXPECT_EQ(1u, f.scheduler.poll(0, 0));
    EXPECT_EQ(0u, f.poller.pending());
    EXPECT_EQ(1u, f.scheduler.queued());
    EXPECT_EQ(1u, f.scheduler.total());

    EXPECT_EQ(1u, f.popIndex(0));

    auto fired = f.scheduler.tryPop(0);
    ASSERT_TRUE(fired);
    EXPECT_EQ(f.states.data(), fired->state);
    EXPECT_EQ(Task::Kind::AsyncReady, fired->kind);

    EXPECT_FALSE(f.scheduler.tryPop(0));
    EXPECT_EQ(0u, f.scheduler.total());

    ::close(fds[0]);
    ::close(fds[1]);
}
#endif

#if defined(OS_LINUX) || defined(OS_DARWIN)
TEST(TaskScheduler, BusyWorkerPicksUpAFiredFdWithinAFewPicks)
{
    Fixture f(1, 2);

    int fds[2];
    ASSERT_EQ(0, ::pipe(fds));
    f.scheduler.push(AsyncTask{.state = f.states.data(), .fd = fds[0], .events = EPOLLIN | EPOLLERR, .timeout_ms = -1});
    char byte = 0;
    ASSERT_EQ(1, ::write(fds[1], &byte, 1));

    size_t picks = 0;
    while (true)
    {
        f.scheduler.push(f.task(1), 0);
        auto popped = f.scheduler.tryPop(0);
        ASSERT_TRUE(popped);
        ASSERT_LT(++picks, 1000u);
        if (popped->kind == Task::Kind::AsyncReady)
            break;
        EXPECT_EQ(&f.states[1], popped->state);
    }

    EXPECT_EQ(0u, f.poller.pending());
    EXPECT_EQ(1u, f.popIndex(0));
    EXPECT_FALSE(f.scheduler.tryPop(0));

    ::close(fds[0]);
    ::close(fds[1]);
}

TEST(TaskScheduler, TryPopPollsWhenTheQueuesAreEmpty)
{
    Fixture f(1, 1);

    int fds[2];
    ASSERT_EQ(0, ::pipe(fds));
    f.scheduler.push(AsyncTask{.state = f.states.data(), .fd = fds[0], .events = EPOLLIN | EPOLLERR, .timeout_ms = -1});

    char byte = 0;
    ASSERT_EQ(1, ::write(fds[1], &byte, 1));

    auto popped = f.scheduler.tryPop(0);
    ASSERT_TRUE(popped);
    EXPECT_EQ(Task::Kind::AsyncReady, popped->kind);
    EXPECT_EQ(0u, f.scheduler.queued());
    EXPECT_EQ(0u, f.poller.pending());

    ::close(fds[0]);
    ::close(fds[1]);
}
#endif

TEST(TaskScheduler, EveryTaskIsPoppedExactlyOnceAcrossThreads)
{
    constexpr size_t workers = 4;
    constexpr size_t per_worker = 2000;

    Fixture f(workers, workers * per_worker);
    std::vector<std::atomic<size_t>> popped(f.states.size());
    std::atomic<size_t> popped_total = 0;

    std::vector<std::thread> threads;
    for (size_t worker = 0; worker < workers; ++worker)
    {
        threads.emplace_back([&, worker]
        {
            for (size_t i = 0; i < per_worker; ++i)
            {
                f.scheduler.push(f.task(worker * per_worker + i), worker);
                if (auto task = f.scheduler.tryPop(worker))
                {
                    ++popped[task->state - f.states.data()];
                    ++popped_total;
                }
            }

            while (popped_total.load() < f.states.size())
            {
                if (auto task = f.scheduler.tryPop(worker))
                {
                    ++popped[task->state - f.states.data()];
                    ++popped_total;
                }
                else
                    std::this_thread::yield();
            }
        });
    }

    for (auto & thread : threads)
        thread.join();

    EXPECT_EQ(0u, f.scheduler.queued());
    for (const auto & count : popped)
        EXPECT_EQ(1u, count.load());
}
