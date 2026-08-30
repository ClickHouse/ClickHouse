#include <gtest/gtest.h>

#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/ISource.h>
#include <Processors/Port.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>

#include <functional>
#include <utility>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")});
}

/// Emits one UInt8 row, then finishes.
class SingleValueSource final : public ISource
{
public:
    SingleValueSource(SharedHeader header_, UInt8 value)
        : ISource(std::move(header_), /*enable_auto_progress=*/false)
    {
        auto col = ColumnUInt8::create();
        col->insertValue(value);
        Columns columns;
        columns.emplace_back(std::move(col));
        chunk.emplace(std::move(columns), 1);
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
            /// Single input slot, reused across every cycle.
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

/// Processor with deferred finish: after its input or output closes it still needs one work() call before it reports Finished
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

/// Lists the batch it retires twice, so `to_remove` violates the set contract while the batch is
/// still unfinished.
class RepeatingRemovalCoordinator final : public IProcessor
{
public:
    explicit RepeatingRemovalCoordinator(SharedHeader header_)
        : IProcessor({}, {Block(*header_)})
        , header(std::move(header_))
    {
    }

    String getName() const override { return "RepeatingRemovalCoordinator"; }

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
            /// The repeated entry is the one that is still unfinished at this point.
            update.to_remove = current_batch;
            update.to_remove.push_back(current_batch.back());
            current_batch.clear();
            outputs.front().finish();
            return update;
        }

        inputs.emplace_back(*header, this);

        /// The closer finishes this processor's input while the transform behind it still owes a
        /// work call, so the batch is unfinished when it is queued for removal.
        auto source = std::make_shared<SingleValueSource>(header, 0);
        auto laggard = std::make_shared<DeferredFinishTransform>(header);
        auto closer = std::make_shared<EarlyClosingTransform>(header);
        connect(source->getOutputs().front(), laggard->getInputs().front());
        connect(laggard->getOutputs().front(), closer->getInputs().front());
        current_batch = {source, closer, laggard};

        connect(closer->getOutputs().front(), inputs.back());
        inputs.back().reopen();
        inputs.back().setNeeded();

        update.to_add = current_batch;
        return update;
    }

private:
    const SharedHeader header;
    Processors current_batch;
};

/// Retires a batch holding a still-unfinished transform, then lists that same transform again on
/// the next call, so the repetition spans two `to_remove` lists instead of appearing within one.
class RepeatingRemovalAcrossCallsCoordinator final : public IProcessor
{
public:
    explicit RepeatingRemovalAcrossCallsCoordinator(SharedHeader header_)
        : IProcessor({}, {Block(*header_)})
        , header(std::move(header_))
    {
    }

    String getName() const override { return "RepeatingRemovalAcrossCallsCoordinator"; }

    Status prepare() override
    {
        auto & output = outputs.front();

        if (output.isFinished())
            return Status::Finished;

        /// The input was disconnected by the retiring call, so it must not be inspected before
        /// the follow-up call has run.
        if (!retired.empty())
            return Status::UpdatePipeline;

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

        if (!retired.empty())
        {
            /// Re-lists a processor whose group is still pending from the previous call.
            update.to_remove.push_back(retired.back());
            retired.clear();
            outputs.front().finish();
            return update;
        }

        if (!inputs.empty())
        {
            disconnect(inputs.back().getOutputPort(), inputs.back());
            /// The laggard still owes a work call here, so the group stays pending.
            retired = current_batch;
            update.to_remove = std::move(current_batch);
            return update;
        }

        inputs.emplace_back(*header, this);

        auto source = std::make_shared<SingleValueSource>(header, 0);
        auto laggard = std::make_shared<DeferredFinishTransform>(header);
        auto closer = std::make_shared<EarlyClosingTransform>(header);
        connect(source->getOutputs().front(), laggard->getInputs().front());
        connect(laggard->getOutputs().front(), closer->getInputs().front());
        current_batch = {source, closer, laggard};

        connect(closer->getOutputs().front(), inputs.back());
        inputs.back().reopen();
        inputs.back().setNeeded();

        update.to_add = current_batch;
        return update;
    }

private:
    const SharedHeader header;
    Processors current_batch;
    Processors retired;
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

/// Retires a whole fan-in of upstreams at once, so that a single `prepare` queues an edge per
/// input slot. Every one of those edges points at a processor of the batch being retired, which
/// widens the window in which a queued edge can be freed. Each chain ends in a processor that
/// needs one more `work` call before it reports `Finished`, so the batch is still unfinished when
/// it is queued for removal and is therefore retired by a later, arbitrary frame.
class WideFanInCyclingCoordinator final : public IProcessor
{
public:
    WideFanInCyclingCoordinator(SharedHeader header_, size_t fan_in_, size_t total_batches_)
        : IProcessor({}, {Block(*header_)})
        , header(std::move(header_))
        , fan_in(fan_in_)
        , total_batches(total_batches_)
    {
    }

    String getName() const override { return "WideFanInCyclingCoordinator"; }

    Status prepare() override
    {
        auto & output = outputs.front();

        if (output.isFinished())
            return Status::Finished;

        if (inputs.empty() || std::ranges::all_of(inputs, [](const auto & input) { return input.isFinished(); }))
            return Status::UpdatePipeline;

        if (!output.canPush())
            return Status::PortFull;

        for (auto & input : inputs)
        {
            if (input.hasData())
            {
                output.push(input.pull(/*set_not_needed=*/true));
                return Status::PortFull;
            }
        }

        for (auto & input : inputs)
            if (!input.isFinished())
                input.setNeeded();

        return Status::NeedData;
    }

    PipelineUpdate updatePipeline() override
    {
        PipelineUpdate update;

        if (inputs.empty())
        {
            for (size_t i = 0; i < fan_in; ++i)
                inputs.emplace_back(*header, this);
        }
        else
        {
            for (auto & input : inputs)
                disconnect(input.getOutputPort(), input);

            update.to_remove = std::move(current_batch);
        }

        if (batches_started == total_batches)
        {
            outputs.front().finish();
            return update;
        }

        Processors next_batch;

        size_t slot = 0;
        for (auto & input : inputs)
        {
            const bool early_close = (slot % 2) == 1;
            auto source = std::make_shared<SingleValueSource>(header, static_cast<UInt8>(slot++));
            next_batch.push_back(source);

            ProcessorPtr tail = source;
            for (size_t depth = 0; depth < 6; ++depth)
            {
                auto laggard = std::make_shared<DeferredFinishTransform>(header);
                connect(tail->getOutputs().front(), laggard->getInputs().front());
                tail = laggard;
                next_batch.push_back(std::move(laggard));
            }

            if (early_close)
            {
                auto closer = std::make_shared<EarlyClosingTransform>(header);
                connect(tail->getOutputs().front(), closer->getInputs().front());
                tail = closer;
                next_batch.push_back(std::move(closer));
            }

            connect(tail->getOutputs().front(), input);
            input.reopen();
            input.setNeeded();
        }

        update.to_add = next_batch;
        current_batch = std::move(next_batch);
        ++batches_started;

        return update;
    }

private:
    const SharedHeader header;
    const size_t fan_in;
    const size_t total_batches;
    size_t batches_started = 0;
    Processors current_batch;
};

}

TEST(Processors, PortDisconnect)
{
    auto header = makeHeader();

    OutputPort out(header);
    InputPort in(header);

    connect(out, in);
    ASSERT_TRUE(out.isConnected());
    ASSERT_TRUE(in.isConnected());

    disconnect(out, in);
    EXPECT_FALSE(out.isConnected());
    EXPECT_FALSE(in.isConnected());
    EXPECT_FALSE(out.hasUpdateInfo());
    EXPECT_FALSE(in.hasUpdateInfo());
}

TEST(Processors, PortDisconnectThrowsIfMismatch)
{
    auto header = makeHeader();
    OutputPort out_a(header);
    InputPort  in_a(header);
    OutputPort out_b(header);
    InputPort  in_b(header);

    connect(out_a, in_a);
    connect(out_b, in_b);

#ifndef DEBUG_OR_SANITIZER_BUILD
    EXPECT_THROW(disconnect(out_a, in_b), Exception);
    EXPECT_THROW(disconnect(out_b, in_a), Exception);
#endif
}

TEST(Processors, UpdatePipeline)
{
    auto header = makeHeader();
    constexpr size_t pulls = 3;

    auto coordinator = std::make_shared<DynamicSourceCoordinator>(header);
    Pipe pipe(coordinator);

    QueryPipeline pipeline(std::move(pipe));
    {
        PullingPipelineExecutor executor(pipeline);

        std::vector<UInt8> values;
        Chunk chunk;
        for (size_t i = 0; i < pulls; ++i)
        {
            ASSERT_TRUE(executor.pull(chunk)) << "executor yielded no chunk at iteration " << i;
            ASSERT_EQ(chunk.getNumRows(), 1u);
            ASSERT_EQ(chunk.getNumColumns(), 1u);
            const auto & col = assert_cast<const ColumnUInt8 &>(*chunk.getColumns().front());
            values.push_back(col.getElement(0));
        }

        EXPECT_EQ(values, (std::vector<UInt8>{0, 1, 2}));
    }

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

TEST(Processors, UpdatePipelineMultipleCoordinatorsMultithreaded)
{
    constexpr size_t num_streams = 16;
    constexpr size_t total_pulls = 1000;
    auto header = makeHeader();

    Pipes pipes;
    std::vector<std::shared_ptr<DynamicSourceCoordinator>> coordinators;
    coordinators.reserve(num_streams);
    for (size_t i = 0; i < num_streams; ++i)
    {
        auto coordinator = std::make_shared<DynamicSourceCoordinator>(header);
        coordinators.push_back(coordinator);
        pipes.emplace_back(std::move(coordinator));
    }

    auto united = Pipe::unitePipes(std::move(pipes));
    united.resize(1, /*strict=*/false, /*min_outstreams_per_resize_after_split=*/0);

    QueryPipeline pipeline(std::move(united));
    pipeline.setNumThreads(num_streams);

    size_t pulled = 0;
    {
        PullingAsyncPipelineExecutor executor(pipeline);

        Chunk chunk;
        while (pulled < total_pulls && executor.pull(chunk))
        {
            if (!chunk)
                continue;
            ASSERT_EQ(chunk.getNumRows(), 1u);
            ASSERT_EQ(chunk.getNumColumns(), 1u);
            ++pulled;
        }

        executor.cancel();
    }

    EXPECT_EQ(pulled, total_pulls);

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

    /// Print statistics
    for (size_t i = 0; i < coordinators.size(); ++i)
        std::cout << "Coordinator #" << i << " Created Sources: " << coordinators.at(i)->totalSourcesCreated() << std::endl;
}

TEST(Processors, UpdatePipelineFanInRemovalNoUseAfterFree)
{
    auto header = makeHeader();

    auto coordinator = std::make_shared<MultiInputRemovingCoordinator>(header);
    Pipe pipe(coordinator);

    QueryPipeline pipeline(std::move(pipe));
    {
        PullingPipelineExecutor executor(pipeline);

        Chunk chunk;
        /// Drain to completion. The point is that execution finishes without an ASan report;
        /// the exact number/content of chunks is not what we assert here.
        while (executor.pull(chunk))
        {
        }
    }

    SUCCEED();
}

TEST(Processors, UpdatePipelineDeferredRemovalOfUnfinishedProcessors)
{
    auto header = makeHeader();

    auto coordinator = std::make_shared<BatchCyclingCoordinator>(header, /*total_batches=*/5);
    Pipe pipe(coordinator);

    QueryPipeline pipeline(std::move(pipe));
    {
        PullingPipelineExecutor executor(pipeline);

        std::vector<UInt8> values;
        Chunk chunk;
        while (executor.pull(chunk))
        {
            ASSERT_EQ(chunk.getNumRows(), 1u);
            const auto & col = assert_cast<const ColumnUInt8 &>(*chunk.getColumns().front());
            values.push_back(col.getElement(0));
        }

        EXPECT_EQ(values, (std::vector<UInt8>{1, 2, 3, 4}));

        for (const auto & weak : coordinator->batchHistory())
            EXPECT_TRUE(weak.expired());
    }

    EXPECT_EQ(coordinator->getInputs().size(), 1u);
}

namespace
{
template <typename Coordinator>
void runRepeatedRemoval()
{
    auto header = makeHeader();
    auto coordinator = std::make_shared<Coordinator>(header);
    Pipe pipe(coordinator);
    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    Chunk chunk;
    while (executor.pull(chunk))
    {
    }
}
}

#ifdef DEBUG_OR_SANITIZER_BUILD

/// A LOGICAL_ERROR aborts here, hence a death test.
TEST(ProcessorsDeathTest, UpdatePipelineRepeatedRemovalIsRejected)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_DEATH(runRepeatedRemoval<RepeatingRemovalCoordinator>(), "listed more than once for removal");
}

TEST(ProcessorsDeathTest, UpdatePipelineRepeatedRemovalAcrossCallsIsRejected)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_DEATH(runRepeatedRemoval<RepeatingRemovalAcrossCallsCoordinator>(), "listed more than once for removal");
}

#else

TEST(Processors, UpdatePipelineRepeatedRemovalIsRejected)
{
    try
    {
        runRepeatedRemoval<RepeatingRemovalCoordinator>();
        ASSERT_TRUE(false) << "Should have thrown.";
    }
    catch (Exception & e)
    {
        ASSERT_TRUE(e.displayText().find("listed more than once for removal") != std::string::npos)
            << "Expected 'listed more than once for removal', got: " << e.displayText();
    }
}

TEST(Processors, UpdatePipelineRepeatedRemovalAcrossCallsIsRejected)
{
    try
    {
        runRepeatedRemoval<RepeatingRemovalAcrossCallsCoordinator>();
        ASSERT_TRUE(false) << "Should have thrown.";
    }
    catch (Exception & e)
    {
        ASSERT_TRUE(e.displayText().find("listed more than once for removal") != std::string::npos)
            << "Expected 'listed more than once for removal', got: " << e.displayText();
    }
}

#endif

/// `updateNode` keeps pending edge updates in a work list that outlives the gaps it makes in
/// `nodes_mutex`, so a concurrent frame can retire a processor and free edges that are still
/// queued elsewhere. Runs many coordinators over many threads, each retiring a wide fan-in of
/// unfinished upstreams, so that removals land in frames other than the one holding the edges.
TEST(Processors, UpdatePipelineConcurrentRemovalDoesNotFreeQueuedEdges)
{
    constexpr size_t num_streams = 4;
    constexpr size_t fan_in = 16;
    constexpr size_t total_batches = 40;
    auto header = makeHeader();

    Pipes pipes;
    for (size_t i = 0; i < num_streams; ++i)
        pipes.emplace_back(std::make_shared<WideFanInCyclingCoordinator>(header, fan_in, total_batches));

    auto united = Pipe::unitePipes(std::move(pipes));
    united.resize(1, /*strict=*/false, /*min_outstreams_per_resize_after_split=*/0);

    QueryPipeline pipeline(std::move(united));
    pipeline.setNumThreads(16);

    size_t pulled = 0;
    {
        PullingAsyncPipelineExecutor executor(pipeline);

        Chunk chunk;
        while (executor.pull(chunk))
        {
            if (!chunk)
                continue;
            ASSERT_EQ(chunk.getNumRows(), 1u);
            ++pulled;
        }
    }

    /// The assertion is the ASan report; this only distinguishes "clean because the pipeline ran"
    /// from "clean because nothing executed".
    EXPECT_GT(pulled, 0u);
}

TEST(Processors, UpdatePipelineRemovalIsNotStrandedByCancellation)
{
    auto header = makeHeader();

    auto coordinator = std::make_shared<DynamicSourceCoordinator>(header);
    Pipe pipe(coordinator);

    QueryPipeline pipeline(std::move(pipe));
    {
        PullingPipelineExecutor executor(pipeline);

        /// Cancel from inside `prepare`, so that the `updatePipeline` retiring the first source is
        /// the very one that observes the cancellation.
        coordinator->setBeforeRetireHook([&] { executor.cancel(); });

        Chunk chunk;
        ASSERT_TRUE(executor.pull(chunk));
        ASSERT_EQ(chunk.getNumRows(), 1u);

        /// Drains whatever is left after the cancellation.
        while (executor.pull(chunk))
        {
        }
    }

    ASSERT_EQ(coordinator->totalSourcesCreated(), 2u);

    /// A source scheduled for removal must leave the pipeline even when the query is cancelled in
    /// the middle of the update, otherwise it stays around until the whole pipeline is destroyed.
    EXPECT_TRUE(coordinator->getSourceWeak(0).expired());
}
