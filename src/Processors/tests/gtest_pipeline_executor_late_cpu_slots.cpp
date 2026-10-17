#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/ConcurrencyControl.h>
#include <base/scope_guard.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/Runtime/V1/PipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/Sinks/EmptySink.h>

#include <condition_variable>
#include <mutex>
#include <set>
#include <thread>

using namespace DB;
using namespace DB::Runtime::V1;

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

/// A CPU slot granted to a running pipeline is used right away: a thread is started at the next task boundary,
/// not only when the pipeline happens to push new tasks to the shared queue (each thread does that after
/// `max_scheduled_local_tasks` local tasks in a row).
TEST(PipelineExecutor, ThreadsForSlotsGrantedAfterStart)
{
    checkThreadsForLateSlots(/* lazy_allocation */ true);
}

TEST(PipelineExecutor, ThreadsForSlotsGrantedAfterStartEagerAllocation)
{
    checkThreadsForLateSlots(/* lazy_allocation */ false);
}

/// A thread downscaled because its preempted slot was not granted back in time is requested again,
/// so that it is started when the slot is granted later.
TEST(PipelineExecutor, ThreadRequestedAfterDownscale)
{
    ExecutorTasks tasks;
    tasks.init(num_streams, 1, std::make_shared<GrantedAllocation>(num_streams), false, false, nullptr, nullptr, nullptr);
    tasks.upscale(0);
    tasks.upscale(1);
    EXPECT_FALSE(tasks.hasThreadsToSpawn());

    tasks.preempt(1);
    tasks.downscale(1);
    EXPECT_TRUE(tasks.hasThreadsToSpawn());

    tasks.upscale(1);
    EXPECT_FALSE(tasks.hasThreadsToSpawn());
    tasks.finish();
}
