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
