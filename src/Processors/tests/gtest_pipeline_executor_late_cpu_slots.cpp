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

/// Shared by the sources of a pipeline: counts the steps and the distinct threads that ran them.
struct LateSlotsState
{
    std::mutex mutex;
    std::condition_variable cv;

    bool first_step_started = false;
    bool slots_released = false;

    /// Counted from the moment the CPU slots were released.
    size_t steps_after_release = 0;
    std::set<std::thread::id> threads_after_release;
    bool done = false;
};

/// Each step sleeps a bit, so that threads overlap. The first step of the pipeline waits until the test releases
/// the CPU slots held by another allocation. The sources stop when all `num_streams` threads ran a step after
/// the release, or after too many steps.
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
        {
            std::unique_lock lock(state.mutex);
            if (!state.first_step_started)
            {
                state.first_step_started = true;
                state.cv.notify_all();
                state.cv.wait_for(lock, std::chrono::seconds(60), [&] { return state.slots_released; });
            }
            else
            {
                ++state.steps_after_release;
                state.threads_after_release.insert(std::this_thread::get_id());
                if (state.threads_after_release.size() >= num_streams || state.steps_after_release >= 10000)
                    state.done = true;
            }
            if (state.done)
                return {};
        }

        std::this_thread::sleep_for(std::chrono::microseconds(500));
        return Chunk(Columns{ColumnUInt8::create(1, static_cast<UInt8>(1))}, 1);
    }

private:
    LateSlotsState & state;
};

size_t stepsUntilAllThreadsRun(bool lazy_allocation)
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
        EXPECT_TRUE(state.cv.wait_for(lock, std::chrono::seconds(60), [&] { return state.first_step_started; }))
            << "The pipeline did not start";
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
    EXPECT_EQ(state.threads_after_release.size(), num_streams);
    return state.steps_after_release;
}

}

/// A CPU slot granted to a running pipeline is used right away: a thread is started at the next task boundary,
/// not only when the pipeline happens to push new tasks to the shared queue (each thread does that after
/// `max_scheduled_local_tasks` local tasks in a row).
TEST(PipelineExecutor, ThreadsForSlotsGrantedAfterStart)
{
    EXPECT_LT(stepsUntilAllThreadsRun(/* lazy_allocation */ true), 32);
}

TEST(PipelineExecutor, ThreadsForSlotsGrantedAfterStartEagerAllocation)
{
    EXPECT_LT(stepsUntilAllThreadsRun(/* lazy_allocation */ false), 32);
}
