#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/Runtime/Engine/Worker.h>
#include <Processors/Executors/Runtime/Engine/WorkerPool.h>
#include <Processors/Port.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <unistd.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")});
}

Chunk makeChunk()
{
    Columns columns;
    columns.emplace_back(ColumnUInt8::create(1, UInt8{1}));
    return Chunk(std::move(columns), 1);
}

/// Makes a chunk in work and pushes it in the next round, chunks_to_push times, then finishes.
class CountingSource final : public IProcessor
{
public:
    explicit CountingSource(size_t chunks_to_push_)
        : IProcessor({}, {OutputPort(makeHeader())})
        , chunks_to_push(chunks_to_push_)
    {
    }

    String getName() const override { return "CountingSource"; }

    Status prepare() override
    {
        auto & output = outputs.front();

        if (chunk)
        {
            if (!output.canPush())
                return Status::PortFull;

            output.push(std::move(*chunk));
            chunk.reset();
            ++pushed;
            return Status::PortFull;
        }

        if (pushed == chunks_to_push)
        {
            output.finish();
            return Status::Finished;
        }

        return Status::Ready;
    }

    void work() override { chunk = makeChunk(); }

private:
    size_t chunks_to_push;
    size_t pushed = 0;
    std::optional<Chunk> chunk;
};

/// Produces its only chunk inside work, then finishes. Throws in work when asked.
class WorkingSource final : public IProcessor
{
public:
    explicit WorkingSource(bool throw_in_work_ = false)
        : IProcessor({}, {OutputPort(makeHeader())})
        , throw_in_work(throw_in_work_)
    {
    }

    String getName() const override { return "WorkingSource"; }

    Status prepare() override
    {
        auto & output = outputs.front();

        if (!produced)
            return Status::Ready;

        if (chunk)
        {
            if (!output.canPush())
                return Status::PortFull;

            output.push(std::move(*chunk));
            chunk.reset();
            return Status::PortFull;
        }

        output.finish();
        return Status::Finished;
    }

    void work() override
    {
        ++work_calls;
        if (throw_in_work)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "work failed");

        chunk = makeChunk();
        produced = true;
    }

    size_t work_calls = 0;

private:
    bool throw_in_work;
    bool produced = false;
    std::optional<Chunk> chunk;
};

#if defined(OS_LINUX) || defined(OS_DARWIN)
/// Asks to wait for its pipe, produces its only chunk in work after the event, then finishes.
class AsyncSource final : public IProcessor
{
public:
    AsyncSource() : IProcessor({}, {OutputPort(makeHeader())})
    {
        EXPECT_EQ(0, ::pipe(fds));
    }

    ~AsyncSource() override
    {
        ::close(fds[0]);
        ::close(fds[1]);
    }

    String getName() const override { return "AsyncSource"; }

    Status prepare() override
    {
        auto & output = outputs.front();

        if (!produced)
            return Status::Async;

        if (chunk)
        {
            if (!output.canPush())
                return Status::PortFull;

            output.push(std::move(*chunk));
            chunk.reset();
            return Status::PortFull;
        }

        output.finish();
        return Status::Finished;
    }

    std::tuple<int, uint32_t, Int64> scheduleForEvent() override
    {
        ++schedule_calls;
        return {fds[0], EPOLLIN | EPOLLERR, -1};
    }

    void onAsyncJobReady() override
    {
        ++ready_calls;
        ready_before_work = work_calls == 0;
    }

    void work() override
    {
        ++work_calls;
        chunk = makeChunk();
        produced = true;
    }

    void fire() const
    {
        char byte = 0;
        EXPECT_EQ(1, ::write(fds[1], &byte, 1));
    }

    size_t schedule_calls = 0;
    size_t ready_calls = 0;
    size_t work_calls = 0;
    bool ready_before_work = false;

private:
    int fds[2] = {-1, -1};
    bool produced = false;
    std::optional<Chunk> chunk;
};
#endif

/// Has no input at first; updatePipeline creates one and a source behind it, then it passes chunks through.
class Expander final : public IProcessor
{
public:
    Expander() : IProcessor({}, {OutputPort(makeHeader())}) {}

    String getName() const override { return "Expander"; }

    Status prepare() override
    {
        auto & output = outputs.front();

        if (inputs.empty())
            return Status::UpdatePipeline;

        auto & input = inputs.front();
        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        if (!output.canPush())
            return Status::PortFull;

        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        output.push(input.pull());
        return Status::PortFull;
    }

    PipelineUpdate updatePipeline() override
    {
        inputs.emplace_back(Block(*makeHeader()), this);
        auto source = std::make_shared<CountingSource>(2);
        connect(source->getOutputs().front(), inputs.back());
        return PipelineUpdate{.to_add = {source}, .to_remove = {}, .to_reconnect = {}};
    }
};

class RecordingSink final : public IProcessor
{
public:
    RecordingSink() : IProcessor({InputPort(makeHeader())}, {}) {}

    String getName() const override { return "RecordingSink"; }

    Status prepare() override
    {
        auto & input = inputs.front();
        if (input.hasData())
        {
            input.pull();
            ++pulled;
        }

        if (input.isFinished())
            return Status::Finished;

        input.setNeeded();
        return Status::NeedData;
    }

    size_t pulled = 0;
};

struct Harness
{
    std::shared_ptr<Processors> processors;
    ExecutingPipeline pipeline;
    Poller poller;
    TaskScheduler scheduler;
    WorkersCoordinator coordinator;
    SlotAllocationPtr cpu_slots;
    WorkerPool pool;
    WorkerSlot slot;
    Worker worker;

    explicit Harness(Processors processors_)
        : processors(std::make_shared<Processors>(std::move(processors_)))
        , pipeline(processors, nullptr, nullptr)
        , scheduler(poller, 1)
        , coordinator(scheduler, poller, 1)
        , cpu_slots(std::make_shared<GrantedAllocation>(1))
        , pool(scheduler, coordinator, pipeline, 1, false)
        , slot(cpu_slots->acquire(), nullptr, nullptr)
        , worker(0, scheduler, coordinator, pipeline, pool)
    {
        for (auto * sink : pipeline.sinks())
        {
            {
                auto round_lock = sink->lock.lockRound();
                sink->lock.setExecuting();
            }
            scheduler.push(Task{.state = sink, .kind = Task::Kind::Prepare});
        }
        coordinator.enter(0);
    }

    void execute(std::atomic_bool * yield_flag = nullptr)
    {
        worker.run(slot, yield_flag);
    }
};

}

TEST(Worker, RunsAPipelineToTheEnd)
{
    auto source = std::make_shared<CountingSource>(3);
    auto sink = std::make_shared<RecordingSink>();
    connect(source->getOutputs().front(), sink->getInputs().front());

    Harness run({source, sink});
    run.execute();

    EXPECT_EQ(3u, sink->pulled);
    EXPECT_TRUE(run.pipeline.allFinished());
    EXPECT_TRUE(run.coordinator.stopped());
    EXPECT_FALSE(run.pipeline.cancelled());
    EXPECT_EQ(0u, run.scheduler.queued());
}

TEST(Worker, WorkRunsBeforeTheRoundsAndIsCounted)
{
    auto source = std::make_shared<WorkingSource>();
    auto sink = std::make_shared<RecordingSink>();
    connect(source->getOutputs().front(), sink->getInputs().front());

    Harness run({source, sink});
    run.execute();

    EXPECT_EQ(1u, source->work_calls);
    EXPECT_EQ(1u, source->getNumExecutedJobs());
    EXPECT_EQ(1u, sink->pulled);
    EXPECT_TRUE(run.pipeline.allFinished());
}

#if defined(OS_LINUX) || defined(OS_DARWIN)
TEST(Worker, AsyncStatusWaitsForTheEventThenWorks)
{
    auto source = std::make_shared<AsyncSource>();
    auto sink = std::make_shared<RecordingSink>();
    connect(source->getOutputs().front(), sink->getInputs().front());

    Harness run({source, sink});
    source->fire();
    run.execute();

    EXPECT_EQ(1u, source->schedule_calls);
    EXPECT_EQ(1u, source->ready_calls);
    EXPECT_EQ(1u, source->work_calls);
    EXPECT_TRUE(source->ready_before_work);
    EXPECT_EQ(1u, sink->pulled);
    EXPECT_TRUE(run.pipeline.allFinished());
    EXPECT_EQ(0u, run.poller.pending());
}
#endif

TEST(Worker, ThrowingWorkLeavesTheLoopWithTheException)
{
    auto source = std::make_shared<WorkingSource>(/*throw_in_work=*/true);
    auto sink = std::make_shared<RecordingSink>();
    connect(source->getOutputs().front(), sink->getInputs().front());

    Harness run({source, sink});
    try
    {
        run.execute();
        FAIL() << "run must throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(ErrorCodes::BAD_ARGUMENTS, e.code());
        EXPECT_TRUE(e.message().contains("While executing WorkingSource")) << e.message();
    }

    EXPECT_FALSE(run.pipeline.exception);
    EXPECT_FALSE(run.pipeline.cancelled());
    EXPECT_FALSE(run.coordinator.stopped());
}

TEST(Worker, UpdatePipelineAddsWiresAndSchedulesTheNewProcessors)
{
    auto expander = std::make_shared<Expander>();
    auto sink = std::make_shared<RecordingSink>();
    connect(expander->getOutputs().front(), sink->getInputs().front());

    Harness run({expander, sink});
    run.execute();

    EXPECT_EQ(3u, run.processors->size());
    EXPECT_EQ(2u, sink->pulled);
    EXPECT_TRUE(run.pipeline.allFinished());
    EXPECT_TRUE(run.coordinator.stopped());
}

TEST(Worker, YieldFlagLeavesTheLoopAfterATask)
{
    auto source = std::make_shared<CountingSource>(3);
    auto sink = std::make_shared<RecordingSink>();
    connect(source->getOutputs().front(), sink->getInputs().front());

    Harness run({source, sink});
    std::atomic_bool yield = true;
    run.execute(&yield);

    EXPECT_FALSE(run.coordinator.stopped());
    EXPECT_FALSE(run.pipeline.allFinished());

    yield = false;
    run.execute();
    EXPECT_EQ(3u, sink->pulled);
    EXPECT_TRUE(run.pipeline.allFinished());
}
