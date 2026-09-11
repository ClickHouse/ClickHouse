#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/IAST.h>
#include <Processors/Executors/Runtime/Executor.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/ISource.h>
#include <Processors/Port.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Common/Exception.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

#include <atomic>
#include <algorithm>
#include <functional>
#include <thread>
#include <utility>
#include <unistd.h>

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

Chunk makeChunk(UInt8 value)
{
    auto col = ColumnUInt8::create();
    col->insertValue(value);
    Columns columns;
    columns.emplace_back(std::move(col));
    return Chunk(std::move(columns), 1);
}

QueryStatusPtr makeQueryStatus(UInt64 max_execution_time_seconds)
{
    ClientInfo client_info;
    client_info.current_query_id = "gtest_executor";
    Settings settings;
    settings.set("max_execution_time", max_execution_time_seconds);
    return std::make_shared<QueryStatus>(
        getContext().context,
        "SELECT 1",
        /*normalized_query_hash_*/ 0,
        client_info,
        /*priority_handle_*/ QueryPriorities::Handle{},
        /*query_slot_*/ nullptr,
        /*memory_reservation_*/ nullptr,
        /*thread_group_*/ nullptr,
        IAST::QueryKind::Select,
        settings,
        clock_gettime_ns(CLOCK_MONOTONIC),
        /*is_internal*/ false);
}

/// Emits one UInt8 row, then finishes.
class SingleValueSource final : public ISource
{
public:
    SingleValueSource(SharedHeader header_, UInt8 value)
        : ISource(std::move(header_), /*enable_auto_progress=*/false)
        , chunk(makeChunk(value))
    {
    }

    String getName() const override { return "SingleValueSource"; }

protected:
    std::optional<Chunk> tryGenerate() override
    {
        return std::exchange(chunk, std::nullopt);
    }

private:
    std::optional<Chunk> chunk;
};

/// Emits 0, 1, ..., count - 1, one row per chunk, then finishes.
class ValuesSource final : public ISource
{
public:
    ValuesSource(SharedHeader header_, size_t count_)
        : ISource(std::move(header_), /*enable_auto_progress=*/false)
        , count(count_)
    {
    }

    String getName() const override { return "ValuesSource"; }

protected:
    std::optional<Chunk> tryGenerate() override
    {
        if (next == count)
            return std::nullopt;

        return makeChunk(static_cast<UInt8>(next++));
    }

private:
    const size_t count;
    size_t next = 0;
};

/// Never finishes on its own.
class EndlessSource final : public ISource
{
public:
    explicit EndlessSource(SharedHeader header_)
        : ISource(std::move(header_), /*enable_auto_progress=*/false)
    {
    }

    String getName() const override { return "EndlessSource"; }

protected:
    std::optional<Chunk> tryGenerate() override
    {
        return makeChunk(0);
    }
};

/// Passes the first chunk through and drops every later one, like a filter that matches once in an endless stream.
class MatchOnceTransform final : public ISimpleTransform
{
public:
    explicit MatchOnceTransform(SharedHeader header_)
        : ISimpleTransform(header_, header_, /*skip_empty_chunks=*/true)
    {
    }

    String getName() const override { return "MatchOnceTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        if (std::exchange(matched, true))
            chunk = Chunk();
    }

private:
    bool matched = false;
};

#if defined(OS_LINUX) || defined(OS_DARWIN)
/// Waits for its pipe, produces its only chunk in work after the event, then finishes.
class PipeSource final : public IProcessor
{
public:
    explicit PipeSource(SharedHeader header_) : IProcessor({}, {Block(*header_)})
    {
        EXPECT_EQ(0, ::pipe(fds));
    }

    ~PipeSource() override
    {
        ::close(fds[0]);
        ::close(fds[1]);
    }

    String getName() const override { return "PipeSource"; }

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

    std::tuple<int, uint32_t, Int64> scheduleForEvent() override { return {fds[0], EPOLLIN | EPOLLERR, -1}; }

    void work() override
    {
        chunk = makeChunk(7);
        produced = true;
    }

    void fire() const
    {
        char byte = 0;
        EXPECT_EQ(1, ::write(fds[1], &byte, 1));
    }

private:
    int fds[2] = {-1, -1};
    bool produced = false;
    std::optional<Chunk> chunk;
};
#endif

class ThrowingSource final : public ISource
{
public:
    explicit ThrowingSource(SharedHeader header_)
        : ISource(std::move(header_), /*enable_auto_progress=*/false)
    {
    }

    String getName() const override { return "ThrowingSource"; }

protected:
    std::optional<Chunk> tryGenerate() override
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "work failed");
    }
};

/// Pulls everything and remembers the values. With a limit, calls on_limit and closes its input after that many chunks.
class CollectingSink final : public IProcessor
{
public:
    explicit CollectingSink(SharedHeader header_, size_t limit_ = 0, std::function<void()> on_limit_ = {})
        : IProcessor({Block(*header_)}, {})
        , limit(limit_)
        , on_limit(std::move(on_limit_))
    {
    }

    String getName() const override { return "CollectingSink"; }

    Status prepare() override
    {
        auto & input = inputs.front();

        if (input.hasData())
        {
            auto chunk = input.pull();
            const auto & col = assert_cast<const ColumnUInt8 &>(*chunk.getColumns().front());
            values.push_back(col.getElement(0));
            ++pulled;

            if (limit && values.size() >= limit)
            {
                if (on_limit)
                    std::exchange(on_limit, {})();

                input.close();
                return Status::Finished;
            }
        }

        if (input.isFinished())
            return Status::Finished;

        input.setNeeded();
        return Status::NeedData;
    }

    std::vector<UInt8> values;
    std::atomic<size_t> pulled = 0;

private:
    const size_t limit;
    std::function<void()> on_limit;
};

/// On each cycle: remove finished upstream, add a fresh one. Never finishes.
class DynamicSourceCoordinator final : public IProcessor
{
public:
    explicit DynamicSourceCoordinator(SharedHeader header_)
        : IProcessor({}, {Block(*header_)})
        , header(std::move(header_))
    {
    }

    String getName() const override { return "DynamicSourceCoordinator"; }

    /// Called once, from `prepare`, right before the cycle that retires the current source.
    void setBeforeRetireHook(std::function<void()> hook) { before_retire_hook = std::move(hook); }

    Status prepare() override
    {
        auto & output = outputs.front();

        if (!current_source)
            return Status::UpdatePipeline;

        auto & input = inputs.back();
        if (input.isFinished())
        {
            if (before_retire_hook)
                std::exchange(before_retire_hook, {})();

            return Status::UpdatePipeline;
        }

        if (!output.canPush())
            return Status::PortFull;

        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }

        output.push(input.pull(/*set_not_needed=*/true));
        return Status::PortFull;
    }

    PipelineUpdate updatePipeline() override
    {
        PipelineUpdate update;

        if (current_source)
        {
            EXPECT_TRUE(inputs.back().isConnected());
            EXPECT_TRUE(inputs.back().isFinished());

            disconnect(current_source->getOutputs().front(), inputs.back());

            EXPECT_FALSE(inputs.back().isConnected());

            update.to_remove.push_back(current_source);
            current_source.reset();
        }
        else
        {
            inputs.emplace_back(*header, this);
        }

        auto new_source = std::make_shared<SingleValueSource>(header, static_cast<UInt8>(source_history.size()));
        source_history.emplace_back(new_source);

        connect(new_source->getOutputs().front(), inputs.back());
        inputs.back().reopen();
        inputs.back().setNeeded();

        EXPECT_TRUE(inputs.back().isConnected());
        EXPECT_FALSE(inputs.back().isFinished());

        current_source = new_source;
        update.to_add.push_back(std::move(new_source));

        return update;
    }

    size_t totalSourcesCreated() const { return source_history.size(); }
    std::weak_ptr<IProcessor> getSourceWeak(size_t idx) const { return source_history.at(idx); }

private:
    const SharedHeader header;
    ProcessorPtr current_source;
    std::vector<std::weak_ptr<IProcessor>> source_history;
    std::function<void()> before_retire_hook;
};

class FinishingSource final : public IProcessor
{
public:
    FinishingSource(SharedHeader header_, size_t fan_out)
        : IProcessor({}, OutputPorts(fan_out, header_))
    {
    }

    String getName() const override { return "FinishingSource"; }

    Status prepare() override
    {
        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }
};

class MultiInputRemovingCoordinator final : public IProcessor
{
public:
    explicit MultiInputRemovingCoordinator(SharedHeader header_)
        : IProcessor({}, {Block(*header_)})
        , header(std::move(header_))
    {
    }

    String getName() const override { return "MultiInputRemovingCoordinator"; }

    Status prepare() override
    {
        if (outputs.front().isFinished())
            return Status::Finished;

        if (!source_added)
            return Status::UpdatePipeline;

        if (std::ranges::all_of(inputs, [](const auto & input) { return input.isFinished(); }))
            return Status::UpdatePipeline;

        return Status::NeedData;
    }

    PipelineUpdate updatePipeline() override
    {
        PipelineUpdate update;

        if (!source_added)
        {
            source = std::make_shared<FinishingSource>(header, 8);
            for (auto & output : source->getOutputs())
            {
                auto & input = inputs.emplace_back(*header, this);
                connect(output, input);
                input.setNeeded();
            }
            update.to_add.push_back(source);
            source_added = true;
            return update;
        }

        for (auto & input : inputs)
            disconnect(input.getOutputPort(), input);

        update.to_remove.push_back(source);
        source.reset();
        outputs.front().finish();
        return update;
    }

private:
    const SharedHeader header;
    ProcessorPtr source;
    bool source_added = false;
};

/// After its input or output closes it still needs one work call before it reports Finished.
class DeferredFinishTransform final : public IProcessor
{
public:
    explicit DeferredFinishTransform(SharedHeader header_)
        : IProcessor({Block(*header_)}, {Block(*header_)})
    {
    }

    String getName() const override { return "DeferredFinishTransform"; }

    Status prepare() override
    {
        auto & input = inputs.front();
        auto & output = outputs.front();

        if (!draining)
        {
            if (output.isFinished() || input.isFinished())
            {
                draining = true;
                return Status::Ready;
            }

            if (!output.canPush())
                return Status::PortFull;

            if (!input.hasData())
            {
                input.setNeeded();
                return Status::NeedData;
            }

            output.push(input.pull(/*set_not_needed=*/true));
            return Status::PortFull;
        }

        if (!drained)
            return Status::Ready;

        input.close();
        output.finish();
        return Status::Finished;
    }

    void work() override { drained = true; }

private:
    bool draining = false;
    bool drained = false;
};

/// Closes its input and finishes its output on the first prepare.
class EarlyClosingTransform final : public IProcessor
{
public:
    explicit EarlyClosingTransform(SharedHeader header_)
        : IProcessor({Block(*header_)}, {Block(*header_)})
    {
    }

    String getName() const override { return "EarlyClosingTransform"; }

    Status prepare() override
    {
        inputs.front().close();
        outputs.front().finish();
        return Status::Finished;
    }
};

/// Cycles source -> deferred-finish (-> early closer for the first batch) sub-pipelines, retiring each batch via to_remove.
class BatchCyclingCoordinator final : public IProcessor
{
public:
    BatchCyclingCoordinator(SharedHeader header_, size_t total_batches_)
        : IProcessor({}, {Block(*header_)})
        , header(std::move(header_))
        , total_batches(total_batches_)
    {
    }

    String getName() const override { return "BatchCyclingCoordinator"; }

    Status prepare() override
    {
        auto & output = outputs.front();

        if (output.isFinished())
            return Status::Finished;

        if (inputs.empty() || inputs.back().isFinished())
            return Status::UpdatePipeline;

        if (!output.canPush())
            return Status::PortFull;

        auto & input = inputs.back();
        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }

        output.push(input.pull(/*set_not_needed=*/true));
        return Status::PortFull;
    }

    PipelineUpdate updatePipeline() override
    {
        PipelineUpdate update;

        if (!inputs.empty())
        {
            disconnect(inputs.back().getOutputPort(), inputs.back());
            update.to_remove = std::move(current_batch);
        }
        else
        {
            inputs.emplace_back(*header, this);
        }

        if (batches_started == total_batches)
        {
            outputs.front().finish();
            return update;
        }

        auto source = std::make_shared<SingleValueSource>(header, static_cast<UInt8>(batches_started));
        auto laggard = std::make_shared<DeferredFinishTransform>(header);
        connect(source->getOutputs().front(), laggard->getInputs().front());
        current_batch = {source, laggard};

        if (batches_started == 0)
        {
            auto closer = std::make_shared<EarlyClosingTransform>(header);
            connect(laggard->getOutputs().front(), closer->getInputs().front());
            current_batch.push_back(closer);
        }

        connect(current_batch.back()->getOutputs().front(), inputs.back());
        inputs.back().reopen();
        inputs.back().setNeeded();

        update.to_add = current_batch;
        batch_history.append_range(current_batch);
        ++batches_started;

        return update;
    }

    const std::vector<std::weak_ptr<IProcessor>> & batchHistory() const { return batch_history; }

private:
    const SharedHeader header;
    const size_t total_batches;
    size_t batches_started = 0;
    Processors current_batch;
    std::vector<std::weak_ptr<IProcessor>> batch_history;
};

/// Gets its inputs from outside one by one, like MergingSortedTransform under an external sort; once told it has them all,
/// pulls one chunk from every input and emits how many it got.
class Collector final : public IProcessor
{
public:
    explicit Collector(SharedHeader header_) : IProcessor({}, {Block(*header_)}) {}

    String getName() const override { return "Collector"; }

    void addInput() { inputs.emplace_back(outputs.front().getHeader(), this); }
    void setHaveAllInputs() { have_all_inputs = true; }

    Status prepare() override
    {
        if (!have_all_inputs)
            return Status::NeedData;

        auto & output = outputs.front();
        if (output.isFinished())
        {
            for (auto & input : inputs)
                input.close();
            return Status::Finished;
        }

        if (emitted)
        {
            output.finish();
            return Status::Finished;
        }

        if (!output.canPush())
            return Status::PortFull;

        bool all_done = true;
        for (auto & input : inputs)
        {
            if (input.isFinished())
                continue;

            input.setNeeded();
            all_done = false;
            if (input.hasData())
                collected += input.pull().getNumRows();
        }

        if (!all_done)
            return Status::NeedData;

        output.push(makeChunk(static_cast<UInt8>(collected)));
        emitted = true;
        return Status::PortFull;
    }

private:
    std::atomic<bool> have_all_inputs = false;
    bool emitted = false;
    size_t collected = 0;
};

/// Spills like MergeSortingTransform: every update adds one source and connects it to a new input of the collector,
/// which itself is added with the first spill and declared in to_reconnect afterwards; the last spill tells the collector it has all inputs.
class Spiller final : public IProcessor
{
public:
    Spiller(SharedHeader header_, size_t spills_)
        : IProcessor({}, {Block(*header_)})
        , header(std::move(header_))
        , spills(spills_)
    {
    }

    String getName() const override { return "Spiller"; }

    Status prepare() override
    {
        if (spills_started < spills)
            return Status::UpdatePipeline;

        auto & input = inputs.front();
        auto & output = outputs.front();

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
        PipelineUpdate update;

        if (!collector)
        {
            collector = std::make_shared<Collector>(header);
            inputs.emplace_back(*header, this);
            connect(collector->getOutputs().front(), inputs.back());
            update.to_add.push_back(collector);
        }
        else
            update.to_reconnect.push_back(collector);

        auto source = std::make_shared<SingleValueSource>(header, static_cast<UInt8>(spills_started));
        collector->addInput();
        connect(source->getOutputs().front(), collector->getInputs().back());
        update.to_add.push_back(source);

        if (++spills_started == spills)
            collector->setHaveAllInputs();

        return update;
    }

private:
    const SharedHeader header;
    const size_t spills;
    size_t spills_started = 0;
    std::shared_ptr<Collector> collector;
};

std::shared_ptr<Processors> chain(const std::vector<ProcessorPtr> & processors)
{
    for (size_t i = 0; i + 1 < processors.size(); ++i)
        connect(processors[i]->getOutputs().front(), processors[i + 1]->getInputs().front());

    return std::make_shared<Processors>(processors.begin(), processors.end());
}

}

TEST(Executor, RunsAConnectedPipeline)
{
    auto header = makeHeader();
    auto source = std::make_shared<SourceFromSingleChunk>(header, makeChunk(1));
    auto sink = std::make_shared<NullSink>(header);

    auto processors = chain({source, sink});
    Executor executor(processors, nullptr);
    executor.execute(1, false);
}

TEST(Executor, UpdatePipeline)
{
    auto header = makeHeader();
    constexpr size_t pulls = 3;

    auto coordinator = std::make_shared<DynamicSourceCoordinator>(header);
    std::optional<Executor> executor;
    auto sink = std::make_shared<CollectingSink>(header, pulls, [&] { executor->cancel(IProcessor::CancelReason::CancelledByUser); });

    auto processors = chain({coordinator, sink});
    executor.emplace(processors, nullptr);
    executor->execute(1, false);

    EXPECT_EQ(sink->values, (std::vector<UInt8>{0, 1, 2}));

    /// One upstream per pull, no extras.
    EXPECT_EQ(coordinator->totalSourcesCreated(), pulls);

    /// All but the last upstream have been removed and destroyed.
    for (size_t i = 0; i + 1 < pulls; ++i)
        EXPECT_TRUE(coordinator->getSourceWeak(i).expired()) << "source #" << i;

    /// Last source is still in use.
    EXPECT_FALSE(coordinator->getSourceWeak(pulls - 1).expired()) << "last source";

    /// Input slot was reused, not grown.
    EXPECT_EQ(coordinator->getInputs().size(), 1u);
    EXPECT_EQ(coordinator->getOutputs().size(), 1u);
}

TEST(Executor, SiblingsArriveInPortOrderOnOneWorker)
{
    auto header = makeHeader();
    constexpr size_t branches = 5;
    auto resize = std::make_shared<ResizeProcessor>(header, branches, 1);
    auto sink = std::make_shared<CollectingSink>(header);

    Processors processors;
    UInt8 value = 0;
    for (auto & input : resize->getInputs())
    {
        auto source = std::make_shared<SingleValueSource>(header, value++);
        connect(source->getOutputs().front(), input);
        processors.push_back(source);
    }
    connect(resize->getOutputs().front(), sink->getInputs().front());
    processors.push_back(resize);
    processors.push_back(sink);

    Executor executor(std::make_shared<Processors>(std::move(processors)), nullptr);
    executor.execute(1, false);

    EXPECT_EQ((std::vector<UInt8>{0, 1, 2, 3, 4}), sink->values);
}

TEST(Executor, OneWorkerGivesTheOtherBranchATurn)
{
    auto header = makeHeader();
    auto resize = std::make_shared<ResizeProcessor>(header, 2, 1);
    auto sink = std::make_shared<CollectingSink>(header, 2);

    Processors processors;
    for (auto & input : resize->getInputs())
    {
        auto source = std::make_shared<EndlessSource>(header);
        auto filter = std::make_shared<MatchOnceTransform>(header);
        connect(source->getOutputs().front(), filter->getInputs().front());
        connect(filter->getOutputs().front(), input);
        processors.push_back(source);
        processors.push_back(filter);
    }
    connect(resize->getOutputs().front(), sink->getInputs().front());
    processors.push_back(resize);
    processors.push_back(sink);

    Executor executor(std::make_shared<Processors>(std::move(processors)), nullptr);
    executor.execute(1, false);

    EXPECT_EQ(2u, sink->pulled);
}

#if defined(OS_LINUX) || defined(OS_DARWIN)
TEST(Executor, OneWorkerPollsWhileAnotherBranchNeverStalls)
{
    auto header = makeHeader();
    auto resize = std::make_shared<ResizeProcessor>(header, 2, 1);
    auto sink = std::make_shared<CollectingSink>(header, 2);

    auto endless = std::make_shared<EndlessSource>(header);
    auto filter = std::make_shared<MatchOnceTransform>(header);
    auto waiting = std::make_shared<PipeSource>(header);
    connect(endless->getOutputs().front(), filter->getInputs().front());
    connect(filter->getOutputs().front(), resize->getInputs().front());
    connect(waiting->getOutputs().front(), resize->getInputs().back());
    connect(resize->getOutputs().front(), sink->getInputs().front());
    waiting->fire();

    Executor executor(std::make_shared<Processors>(Processors{endless, filter, waiting, resize, sink}), nullptr);
    executor.execute(1, false);

    EXPECT_EQ(2u, sink->pulled);
    EXPECT_TRUE(std::ranges::contains(sink->values, 7));
}
#endif

TEST(Executor, UpdatePipelineMultipleCoordinatorsMultithreaded)
{
    constexpr size_t num_streams = 16;
    constexpr size_t total_pulls = 1000;
    auto header = makeHeader();

    std::vector<std::shared_ptr<DynamicSourceCoordinator>> coordinators;
    auto resize = std::make_shared<ResizeProcessor>(header, num_streams, 1);
    std::optional<Executor> executor;
    auto sink = std::make_shared<CollectingSink>(header, total_pulls, [&] { executor->cancel(IProcessor::CancelReason::CancelledByUser); });

    Processors processors;
    for (auto & input : resize->getInputs())
    {
        auto coordinator = std::make_shared<DynamicSourceCoordinator>(header);
        connect(coordinator->getOutputs().front(), input);
        coordinators.push_back(coordinator);
        processors.push_back(coordinator);
    }
    connect(resize->getOutputs().front(), sink->getInputs().front());
    processors.push_back(resize);
    processors.push_back(sink);

    auto processors_ptr = std::make_shared<Processors>(std::move(processors));
    executor.emplace(processors_ptr, nullptr);
    executor->execute(num_streams, false);

    EXPECT_EQ(sink->pulled, total_pulls);

    /// Every pulled chunk came from exactly one cycle of some coordinator.
    size_t produced = 0;
    for (const auto & coordinator : coordinators)
    {
        produced += coordinator->totalSourcesCreated();
        EXPECT_EQ(coordinator->getInputs().size(), 1u);
        EXPECT_EQ(coordinator->getOutputs().size(), 1u);

        /// At most one source (the currently-live one) is still alive per coordinator.
        size_t alive = 0;
        for (size_t i = 0; i < coordinator->totalSourcesCreated(); ++i)
            if (!coordinator->getSourceWeak(i).expired())
                ++alive;
        EXPECT_LE(alive, 1u);
    }
    EXPECT_GE(produced, total_pulls);
}

TEST(Executor, UpdatePipelineFanInRemovalNoUseAfterFree)
{
    auto header = makeHeader();
    auto coordinator = std::make_shared<MultiInputRemovingCoordinator>(header);
    auto sink = std::make_shared<CollectingSink>(header);

    auto processors = chain({coordinator, sink});
    Executor executor(processors, nullptr);
    executor.execute(1, false);

    EXPECT_TRUE(sink->values.empty());
}

TEST(Executor, UpdatePipelineDeferredRemovalOfUnfinishedProcessors)
{
    auto header = makeHeader();
    auto coordinator = std::make_shared<BatchCyclingCoordinator>(header, /*total_batches=*/5);
    auto sink = std::make_shared<CollectingSink>(header);

    auto processors = chain({coordinator, sink});
    Executor executor(processors, nullptr);
    executor.execute(1, false);

    EXPECT_EQ(sink->values, (std::vector<UInt8>{1, 2, 3, 4}));

    for (const auto & weak : coordinator->batchHistory())
        EXPECT_TRUE(weak.expired());

    EXPECT_EQ(coordinator->getInputs().size(), 1u);
}

TEST(Executor, UpdatePipelineAddsInputsToAnExistingProcessor)
{
    auto header = makeHeader();
    constexpr size_t spills = 5;
    auto spiller = std::make_shared<Spiller>(header, spills);
    auto sink = std::make_shared<CollectingSink>(header);

    auto processors = chain({spiller, sink});
    Executor executor(processors, nullptr);
    executor.execute(2, false);

    EXPECT_EQ(sink->values, (std::vector<UInt8>{spills}));
    EXPECT_EQ(processors->size(), 2 + 1 + spills);
}

TEST(Executor, CancelInsidePrepareStopsBeforeTheUpdate)
{
    auto header = makeHeader();
    auto coordinator = std::make_shared<DynamicSourceCoordinator>(header);
    auto sink = std::make_shared<CollectingSink>(header);

    auto processors = chain({coordinator, sink});
    Executor executor(processors, nullptr);

    /// Cancel from inside `prepare`, right before the cycle that would retire the first source.
    coordinator->setBeforeRetireHook([&] { executor.cancel(IProcessor::CancelReason::CancelledByUser); });
    executor.execute(1, false);

    EXPECT_EQ(sink->values, (std::vector<UInt8>{0}));
    EXPECT_EQ(coordinator->totalSourcesCreated(), 1u);
    EXPECT_TRUE(coordinator->isCancelled());
    EXPECT_FALSE(coordinator->getSourceWeak(0).expired());
}

TEST(Executor, CancelFromAnotherThreadStopsTheExecution)
{
    auto header = makeHeader();
    auto source = std::make_shared<EndlessSource>(header);
    auto sink = std::make_shared<CollectingSink>(header);

    auto processors = chain({source, sink});
    Executor executor(processors, nullptr);

    std::thread runner([&] { executor.execute(2, false); });

    while (sink->pulled == 0)
        std::this_thread::yield();

    executor.cancel(IProcessor::CancelReason::CancelledByUser);
    runner.join();

    EXPECT_TRUE(source->isCancelled());
    EXPECT_TRUE(sink->isCancelled());
}

TEST(Executor, ExceptionInWorkIsRethrown)
{
    auto header = makeHeader();
    auto source = std::make_shared<ThrowingSource>(header);
    auto sink = std::make_shared<CollectingSink>(header);

    auto processors = chain({source, sink});
    Executor executor(processors, nullptr);

    try
    {
        executor.execute(2, false);
        FAIL() << "execute must throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(ErrorCodes::BAD_ARGUMENTS, e.code());
        EXPECT_TRUE(e.message().contains("While executing ThrowingSource")) << e.message();
    }

    EXPECT_TRUE(sink->isCancelled());
}

TEST(Executor, TimeoutCancelsAndThrows)
{
    auto header = makeHeader();
    auto source = std::make_shared<EndlessSource>(header);
    auto sink = std::make_shared<CollectingSink>(header);

    auto processors = chain({source, sink});
    Executor executor(processors, makeQueryStatus(/*max_execution_time_seconds=*/1));

    try
    {
        executor.execute(2, false);
        FAIL() << "execute must throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(ErrorCodes::TIMEOUT_EXCEEDED, e.code());
    }

    EXPECT_TRUE(source->isCancelled());
    EXPECT_GT(sink->pulled, 0u);
}

TEST(Executor, ExecuteStepYieldsAndFinishes)
{
    auto header = makeHeader();
    auto source = std::make_shared<ValuesSource>(header, 3);
    auto sink = std::make_shared<CollectingSink>(header);

    auto processors = chain({source, sink});
    Executor executor(processors, nullptr);

    std::atomic_bool yield = true;
    EXPECT_TRUE(executor.executeUntil(&yield));
    EXPECT_LE(sink->values.size(), 1u);

    EXPECT_TRUE(executor.executeUntil(&yield));
    EXPECT_LE(sink->values.size(), 1u);

    yield = false;
    EXPECT_FALSE(executor.executeUntil(&yield));
    EXPECT_EQ(sink->values, (std::vector<UInt8>{0, 1, 2}));
}
