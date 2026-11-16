#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/ConcurrencyControl.h>
#include <base/scope_guard.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/Sinks/EmptySink.h>

#include <condition_variable>
#include <mutex>
#include <set>
#include <thread>

#include <unistd.h>

using namespace DB;

namespace
{

constexpr size_t num_streams = 4;
constexpr auto timeout = std::chrono::seconds(60);

struct LateSlotsState
{
    std::mutex mutex;
    std::condition_variable cv;

    bool first_step_started = false;
    bool slots_released = false;

    /// Threads that ran a step after the release.
    std::set<std::thread::id> threads;
    bool done = false;
    bool timed_out = false;
};

/// The first step of the pipeline waits until the test releases the CPU slots held by another allocation.
/// Every later step waits until `num_streams` threads are in it. A thread is started for a granted slot only
/// at a task boundary of a running thread, so without one the only running thread waits here for the timeout.
class LateSlotsSource : public ISource
{
public:
    LateSlotsSource(SharedHeader header, LateSlotsState & state_)
        : ISource(std::move(header))
        , state(state_)
    {
    }

    String getName() const override { return "LateSlotsSource"; }

protected:
    Chunk generate() override
    {
        std::unique_lock lock(state.mutex);
        if (!state.first_step_started)
        {
            state.first_step_started = true;
            state.cv.notify_all();
            state.cv.wait_for(lock, timeout, [&] { return state.slots_released; });
            return Chunk(Columns{ColumnUInt8::create(1, static_cast<UInt8>(1))}, 1);
        }

        if (!state.done)
        {
            state.threads.insert(std::this_thread::get_id());
            if (state.threads.size() >= num_streams)
                state.done = true;
            else if (!state.cv.wait_for(lock, timeout, [&] { return state.done; }))
                state.done = state.timed_out = true;
            state.cv.notify_all();
        }
        return {};
    }

private:
    LateSlotsState & state;
};

void checkThreadsForLateSlots(bool lazy_allocation)
{
    auto & concurrency_control = ConcurrencyControl::instance();
    const String prev_scheduler = concurrency_control.getScheduler();
    const bool prev_lazy_allocation = concurrency_control.getLazyAllocation();
    SCOPE_EXIT({
        concurrency_control.setMaxConcurrency(UnlimitedSlots);
        concurrency_control.setScheduler(prev_scheduler);
        concurrency_control.setLazyAllocation(prev_lazy_allocation);
    });
    concurrency_control.setScheduler("fair_round_robin");
    concurrency_control.setLazyAllocation(lazy_allocation);
    concurrency_control.setMaxConcurrency(num_streams);

    /// Another query holds all the competing slots, so the pipeline starts with its master thread only.
    SlotAllocationPtr other_query = concurrency_control.allocate(0, num_streams);

    LateSlotsState state;
    SharedHeader header = std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")});
    auto processors = std::make_shared<Processors>();
    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<LateSlotsSource>(header, state);
        auto sink = std::make_shared<EmptySink>(source->getPort().getSharedHeader());
        connect(source->getPort(), sink->getPort());
        processors->emplace_back(std::move(source));
        processors->emplace_back(std::move(sink));
    }

    QueryStatusPtr element;
    PipelineExecutor executor(processors, element);
    std::thread query([&] { executor.execute(num_streams, /* concurrency_control */ true); });

    {
        std::unique_lock lock(state.mutex);
        EXPECT_TRUE(state.cv.wait_for(lock, timeout, [&] { return state.first_step_started; })) << "The pipeline did not start";
    }

    /// The other query finishes: its slots are granted to the pipeline, which is in the middle of execution.
    other_query.reset();
    {
        std::lock_guard lock(state.mutex);
        state.slots_released = true;
    }
    state.cv.notify_all();

    query.join();

    std::lock_guard lock(state.mutex);
    EXPECT_FALSE(state.timed_out);
    EXPECT_EQ(state.threads.size(), num_streams);
}

}

/// A CPU slot granted to a running pipeline gets a thread at the next task boundary,
/// even if no step pushes tasks to the shared queue.
TEST(PipelineExecutor, ThreadsForSlotsGrantedAfterStart)
{
    checkThreadsForLateSlots(/* lazy_allocation */ true);
}

TEST(PipelineExecutor, ThreadsForSlotsGrantedAfterStartEagerAllocation)
{
    checkThreadsForLateSlots(/* lazy_allocation */ false);
}

/// A thread requested in place of a downscaled one is kept until a slot is granted and the thread is started.
TEST(PipelineExecutor, ThreadRequestedAfterDownscale)
{
    ExecutorTasks tasks;
    tasks.init(num_streams, 1, std::make_shared<GrantedAllocation>(num_streams), false, false, nullptr, nullptr);
    tasks.upscale(0);
    tasks.upscale(1);
    EXPECT_FALSE(tasks.hasThreadsToSpawn());

    tasks.preempt(1);
    tasks.downscale(1);
    EXPECT_FALSE(tasks.hasThreadsToSpawn());
    tasks.requestThreads(1);
    EXPECT_TRUE(tasks.hasThreadsToSpawn());

    tasks.upscale(1);
    EXPECT_FALSE(tasks.hasThreadsToSpawn());
    tasks.finish();
}

/// A thread that finds no queued task to run drops the threads requested for queued tasks:
/// none are left for them.
TEST(PipelineExecutor, RequestedThreadsDroppedWhenThreadGoesIdle)
{
    ExecutorTasks tasks;
    tasks.init(num_streams, 1, std::make_shared<GrantedAllocation>(num_streams), false, false, nullptr, nullptr);
    tasks.upscale(0);
    tasks.upscale(1);
    tasks.requestThreads(2);
    EXPECT_TRUE(tasks.hasThreadsToSpawn());

    /// One of the two running threads waits for a task; the queues are empty.
    std::thread idle([&] { tasks.tryGetTask(tasks.getThreadContext(1)); });
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (tasks.hasThreadsToSpawn() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    EXPECT_FALSE(tasks.hasThreadsToSpawn());

    tasks.finish();
    idle.join();
}

#if defined(OS_LINUX) || defined(OS_DARWIN)
namespace
{

/// Waits for a pipe that is never written to.
class PendingAsyncProcessor : public IProcessor
{
public:
    explicit PendingAsyncProcessor(int fd_) : fd(fd_) {}
    String getName() const override { return "PendingAsyncProcessor"; }
    Status prepare() override { return Status::Async; }
    int schedule() override { return fd; }

private:
    int fd;
};

}

/// Requested threads are kept while asynchronous tasks are pending: they can run the tasks once these are ready.
TEST(PipelineExecutor, RequestedThreadsKeptWhileAsyncTasksPending)
{
    int fds[2];
    ASSERT_EQ(pipe(fds), 0);
    SCOPE_EXIT({
        close(fds[0]);
        close(fds[1]);
    });
    Processors processors{std::make_shared<PendingAsyncProcessor>(fds[0])};
    ExecutingGraph::Node node(processors.begin(), 0);

    ExecutorTasks tasks;
    tasks.init(num_streams, 1, std::make_shared<GrantedAllocation>(num_streams), false, false, nullptr, nullptr);
    tasks.upscale(0);
    tasks.upscale(1);

    ExecutorTasks::Queue queue;
    ExecutorTasks::Queue async_queue;
    async_queue.push(&node);
    tasks.pushTasks(queue, async_queue, tasks.getThreadContext(0));
    EXPECT_TRUE(tasks.hasThreadsToSpawn());

    /// One of the two running threads waits for a task; only the asynchronous one is pending.
    std::thread idle([&] { tasks.tryGetTask(tasks.getThreadContext(1)); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_TRUE(tasks.hasThreadsToSpawn());

    tasks.finish();
    idle.join();
}
#endif
