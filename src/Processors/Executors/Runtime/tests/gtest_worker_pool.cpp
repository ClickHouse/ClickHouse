#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/Runtime/Engine/WorkerPool.h>
#include <Processors/Port.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <set>
#include <thread>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int TIMEOUT_EXCEEDED;
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

/// Opens when `needed` threads have arrived or when the test opens it; a thread that waits too long throws.
class Gate
{
public:
    explicit Gate(size_t needed_) : needed(needed_) {}

    void pass()
    {
        std::unique_lock lock(mutex);
        if (++arrived >= needed)
        {
            open = true;
            opened.notify_all();
        }

        if (!opened.wait_for(lock, std::chrono::seconds(30), [this] { return open; }))
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "The gate did not open");
    }

    void openNow()
    {
        std::lock_guard lock(mutex);
        open = true;
        opened.notify_all();
    }

private:
    std::mutex mutex;
    std::condition_variable opened;
    const size_t needed;
    size_t arrived = 0;
    bool open = false;
};

/// Produces its only chunk in work after passing the gate, then finishes. Remembers the thread.
class GatedSource final : public IProcessor
{
public:
    GatedSource(Gate & gate_, std::mutex & threads_mutex_, std::set<std::thread::id> & threads_)
        : IProcessor({}, {OutputPort(makeHeader())})
        , gate(gate_)
        , threads_mutex(threads_mutex_)
        , threads(threads_)
    {
    }

    String getName() const override { return "GatedSource"; }

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
        {
            std::lock_guard lock(threads_mutex);
            threads.insert(std::this_thread::get_id());
        }
        gate.pass();
        chunk = makeChunk();
        produced = true;
    }

private:
    Gate & gate;
    std::mutex & threads_mutex;
    std::set<std::thread::id> & threads;
    bool produced = false;
    std::optional<Chunk> chunk;
};

/// Asks for work once and throws in it.
class ThrowingSource final : public IProcessor
{
public:
    ThrowingSource() : IProcessor({}, {OutputPort(makeHeader())}) {}

    String getName() const override { return "ThrowingSource"; }

    Status prepare() override
    {
        if (asked)
            return Status::PortFull;

        asked = true;
        return Status::Ready;
    }

    void work() override
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "work failed");
    }

private:
    bool asked = false;
};

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

/// Pulls everything and counts the chunks.
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

    std::atomic<size_t> pulled = 0;
};

struct Fixture
{
    std::shared_ptr<Processors> processors;
    ExecutingPipeline pipeline;
    Poller poller;
    TaskScheduler scheduler;
    WorkersCoordinator coordinator;
    WorkerPool pool;

    Fixture(Processors processors_, size_t max_threads)
        : processors(std::make_shared<Processors>(std::move(processors_)))
        , pipeline(processors, nullptr, nullptr)
        , scheduler(poller, max_threads)
        , coordinator(scheduler, poller, max_threads)
        , pool(scheduler, coordinator, pipeline, max_threads, false)
    {
        for (auto * sink : pipeline.sinks())
        {
            {
                auto round_lock = sink->lock.lockRound();
                sink->lock.setExecuting();
            }
            scheduler.push(Task{.state = sink, .kind = Task::Kind::Prepare});
        }
    }
};

Processors chain(std::vector<std::shared_ptr<IProcessor>> sources, std::vector<std::shared_ptr<RecordingSink>> sinks)
{
    Processors processors;
    for (size_t i = 0; i < sources.size(); ++i)
    {
        connect(sources[i]->getOutputs().front(), sinks[i]->getInputs().front());
        processors.push_back(sources[i]);
        processors.push_back(sinks[i]);
    }
    return processors;
}

}

TEST(WorkerPool, GrowsWhenThereIsMoreWorkThanWorkersAndStopsAtMaxThreads)
{
    Gate gate(2);
    std::mutex threads_mutex;
    std::set<std::thread::id> threads;

    std::vector<std::shared_ptr<IProcessor>> sources;
    std::vector<std::shared_ptr<RecordingSink>> sinks;
    for (size_t i = 0; i < 4; ++i)
    {
        sources.push_back(std::make_shared<GatedSource>(gate, threads_mutex, threads));
        sinks.push_back(std::make_shared<RecordingSink>());
    }

    Fixture f(chain(sources, sinks), 2);
    f.pool.run();
    f.pool.stop();

    ASSERT_FALSE(f.pipeline.exception) << getExceptionMessage(f.pipeline.exception, false);
    EXPECT_TRUE(f.pipeline.allFinished());
    EXPECT_TRUE(f.coordinator.stopped());
    for (const auto & sink : sinks)
        EXPECT_EQ(1u, sink->pulled);

    EXPECT_EQ(2u, threads.size());
    EXPECT_EQ(0u, f.coordinator.registered());
}

TEST(WorkerPool, IdleWorkerStaysRegistered)
{
    Gate gate(3);
    std::mutex threads_mutex;
    std::set<std::thread::id> threads;

    auto gated = std::make_shared<GatedSource>(gate, threads_mutex, threads);
    auto quick = std::make_shared<CountingSource>(1);
    auto gated_sink = std::make_shared<RecordingSink>();
    auto quick_sink = std::make_shared<RecordingSink>();

    Fixture f(chain({gated, quick}, {gated_sink, quick_sink}), 2);

    std::thread runner([&]
    {
        f.pool.run();
        f.pool.stop();
    });

    while (f.coordinator.idle() == 0)
        std::this_thread::yield();

    EXPECT_EQ(1u, quick_sink->pulled);
    EXPECT_EQ(2u, f.coordinator.registered());

    gate.openNow();
    runner.join();

    ASSERT_FALSE(f.pipeline.exception) << getExceptionMessage(f.pipeline.exception, false);
    EXPECT_TRUE(f.pipeline.allFinished());
    EXPECT_EQ(1u, gated_sink->pulled);
    EXPECT_EQ(0u, f.coordinator.registered());
}

TEST(WorkerPool, ThrowingWorkFailsThePipelineAndStopsTheWorkers)
{
    auto source = std::make_shared<ThrowingSource>();
    auto sink = std::make_shared<RecordingSink>();

    Fixture f(chain({source}, {sink}), 2);
    f.pool.run();
    f.pool.stop();

    ASSERT_TRUE(f.pipeline.exception);
    EXPECT_EQ(ErrorCodes::BAD_ARGUMENTS, getExceptionErrorCode(f.pipeline.exception));
    EXPECT_TRUE(f.pipeline.cancelled());
    EXPECT_TRUE(f.coordinator.stopped());
    EXPECT_TRUE(sink->isCancelled());
    EXPECT_FALSE(f.pipeline.allFinished());
    EXPECT_EQ(0u, f.coordinator.registered());
}

TEST(WorkerPool, RunUntilYieldsAndContinuesOnTheSameSlot)
{
    auto source = std::make_shared<CountingSource>(3);
    auto sink = std::make_shared<RecordingSink>();

    Fixture f(chain({source}, {sink}), 1);

    std::atomic_bool yield = true;
    f.pool.runUntil(&yield);
    EXPECT_FALSE(f.coordinator.stopped());
    EXPECT_FALSE(f.pipeline.allFinished());

    yield = false;
    f.pool.runUntil(&yield);
    f.pool.stop();
    EXPECT_TRUE(f.coordinator.stopped());
    EXPECT_TRUE(f.pipeline.allFinished());
    EXPECT_EQ(3u, sink->pulled);
}
