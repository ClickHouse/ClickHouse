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

    Status prepare() override
    {
        auto & output = outputs.front();

        if (!current_source)
            return Status::UpdatePipeline;

        auto & input = inputs.back();
        if (input.isFinished())
            return Status::UpdatePipeline;

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
};

}

/// A source with FAN_OUT output ports. Every prepare() finishes all of its outputs at once,
/// so a single prepare pushes FAN_OUT direct-edges (source -> consumer) into updateNode's local
/// `updated_edges` work-list in one sweep. That is the ingredient the bug needs: several edges
/// belonging to ONE node queued together, so removing that node leaves the not-yet-drained ones
/// dangling.
class MultiOutputFinishingSource final : public IProcessor
{
public:
    static constexpr size_t fan_out = 8;

    explicit MultiOutputFinishingSource(SharedHeader header_)
        : IProcessor({}, OutputPorts(fan_out, header_))
    {
    }

    String getName() const override { return "MultiOutputFinishingSource"; }

    Status prepare() override
    {
        /// Finish every output in this single prepare. The graph then queues one direct-edge per
        /// output into updateNode's `updated_edges` in the same sweep.
        for (auto & output : outputs)
            output.finish();
        return Status::Finished;
    }
};

/// Reproduces the heap-use-after-free in ExecutingGraph::updateNode (STID 2837-40ba).
///
/// updateNode keeps a LOCAL work-list of Edge* (`updated_edges`), drained only after the
/// processor work-list empties. The source above finishes all FAN_OUT outputs in one prepare, so
/// FAN_OUT direct-edges (source -> this coordinator) are pushed into `updated_edges` together.
/// Draining the FIRST of those edges prepares the coordinator, which returns UpdatePipeline and
/// removes the source: `removeNode` destroys the source's own edge list, freeing every one of
/// those FAN_OUT edges. The FAN_OUT-1 edges still queued in `updated_edges` are now dangling, and
/// the next pop dereferences `edge->to` -> use-after-free. The fix scrubs the freed edges from the
/// work-list right after updatePipeline, so the stale pointers are never dereferenced.
class MultiInputRemovingCoordinator final : public IProcessor
{
public:
    explicit MultiInputRemovingCoordinator(SharedHeader header_)
        : IProcessor({}, {Block(*header_)})   /// starts as a source (no inputs); inputs added at runtime
        , header(std::move(header_))
    {
    }

    String getName() const override { return "MultiInputRemovingCoordinator"; }

    Status prepare() override
    {
        auto & output = outputs.front();

        if (output.isFinished())
        {
            for (auto & input : inputs)
                if (input.isConnected())
                    input.close();
            return Status::Finished;
        }

        /// Source not attached yet -> ask for a pipeline update to add it.
        if (!source_added)
            return Status::UpdatePipeline;

        if (source_removed)
        {
            output.finish();
            return Status::Finished;
        }

        /// The source finished (all inputs finished). Its FAN_OUT direct-edges are now queued in
        /// updateNode's local work-list. Remove the source now so those queued edges dangle.
        bool all_finished = true;
        for (auto & input : inputs)
        {
            input.setNeeded();
            if (!input.isFinished())
                all_finished = false;
        }

        if (all_finished)
            return Status::UpdatePipeline;

        return Status::NeedData;
    }

    PipelineUpdate updatePipeline() override
    {
        PipelineUpdate update;

        if (!source_added)
        {
            /// Attach one multi-output source: source output i -> a fresh input i of this coordinator.
            source = std::make_shared<MultiOutputFinishingSource>(header);
            auto output_it = source->getOutputs().begin();
            for (size_t i = 0; i < MultiOutputFinishingSource::fan_out; ++i, ++output_it)
            {
                auto & input = inputs.emplace_back(*header, this);
                connect(*output_it, input);
                input.setNeeded();
            }
            update.to_add.push_back(source);
            source_added = true;
            return update;
        }

        /// Source finished -> remove it. Disconnect every fan-out output from our inputs first, so
        /// `to_remove` holds a processor already detached from us (the IProcessor::updatePipeline
        /// contract: returned processors must be disconnected, see MergeTreeCommitOrderSequentialSource).
        /// Disconnecting only severs the port linkage; removeNode still destroys the source node's
        /// own graph Edge objects, freeing all FAN_OUT of them at once -- which is what leaves the
        /// stale Edge* dangling in updateNode's work-list and reproduces the use-after-free.
        for (auto & input : inputs)
            disconnect(input.getOutputPort(), input);

        update.to_remove.push_back(source);
        source.reset();
        source_removed = true;
        outputs.front().finish();
        return update;
    }

private:
    const SharedHeader header;
    ProcessorPtr source;
    bool source_added = false;
    bool source_removed = false;
};

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

/// Regression test for the heap-use-after-free in ExecutingGraph::updateNode (STID 2837-40ba):
/// a stale Edge* to a removed source is left in updateNode's local work-list and dereferenced.
/// Without the fix this crashes under ASAN; with the fix it drains cleanly.
TEST(Processors, UpdatePipelineFanInRemovalNoUseAfterFree)
{
    auto header = makeHeader();

    auto coordinator = std::make_shared<MultiInputRemovingCoordinator>(header);
    Pipe pipe(coordinator);

    QueryPipeline pipeline(std::move(pipe));
    {
        PullingPipelineExecutor executor(pipeline);

        Chunk chunk;
        /// Drain to completion. The point is that execution finishes without an ASAN report;
        /// the exact number/content of chunks is not what we assert here.
        while (executor.pull(chunk))
        {
        }
    }

    SUCCEED();
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
