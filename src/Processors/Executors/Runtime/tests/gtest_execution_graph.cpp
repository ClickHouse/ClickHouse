#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/Runtime/Pipeline/ExecutionGraph.h>
#include <Processors/Port.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <vector>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")});
}

class Source final : public IProcessor
{
public:
    Source() : IProcessor({}, {OutputPort(makeHeader())}) {}
    String getName() const override { return "Source"; }
    Status prepare() override { return Status::Finished; }
    OutputPort & output() { return outputs.front(); }
};

class Sink final : public IProcessor
{
public:
    Sink() : IProcessor({InputPort(makeHeader())}, {}) {}
    String getName() const override { return "Sink"; }
    Status prepare() override { return Status::Finished; }
    InputPort & input() { return inputs.front(); }
};

std::vector<IProcessor *> visited(ExecutionGraph & graph)
{
    std::vector<IProcessor *> result;
    graph.forEachProcessor([&](IProcessor & processor) { result.push_back(&processor); });
    return result;
}

}

TEST(ExecutionGraph, WiresBothEndsAndKeepsTheSharedList)
{
    auto source = std::make_shared<Source>();
    auto sink = std::make_shared<Sink>();
    connect(source->output(), sink->input());

    auto processors = std::make_shared<Processors>(Processors{source, sink});
    ExecutionGraph graph(processors);

    EXPECT_EQ((std::vector<IProcessor *>{source.get(), sink.get()}), visited(graph));

    ProcessorState & source_state = graph.getState(*source);
    ProcessorState & sink_state = graph.getState(*sink);
    EXPECT_EQ(source.get(), source_state.processor);
    EXPECT_EQ(&source_state, &source->output().getUpdateChannel().getOwner());
    EXPECT_EQ(&sink_state, &sink->input().getUpdateChannel().getOwner());

    EXPECT_FALSE(graph.allFinished());
    ASSERT_TRUE(source_state.lock.tryLock());
    source_state.lock.finish();
    EXPECT_FALSE(graph.allFinished());
    ASSERT_TRUE(sink_state.lock.tryLock());
    sink_state.lock.finish();
    EXPECT_TRUE(graph.allFinished());

    EXPECT_FALSE(graph.dump().empty());
}

TEST(ExecutionGraph, PeerOutsideTheListIsLogicalError)
{
    auto source = std::make_shared<Source>();
    auto omitted_sink = std::make_shared<Sink>();
    connect(source->output(), omitted_sink->input());

    auto processors = std::make_shared<Processors>(Processors{source});
    try
    {
        ExecutionGraph graph(processors);
        FAIL() << "expected a LOGICAL_ERROR";
    }
    catch (const Exception & e)
    {
        EXPECT_NE(std::string::npos, e.message().find("was found as output for processor"));
    }
}

TEST(ExecutionGraph, AddWiresTheGroupAndTheRequester)
{
    auto source = std::make_shared<Source>();
    auto processors = std::make_shared<Processors>(Processors{source});
    ExecutionGraph graph(processors);
    ProcessorState & source_state = graph.getState(*source);
    EXPECT_FALSE(source->output().getUpdateChannel().isConnected());

    auto sink = std::make_shared<Sink>();
    connect(source->output(), sink->input());
    graph.add(source_state, {sink});

    EXPECT_EQ((Processors{source, sink}), *processors);
    EXPECT_THROW(graph.add(source_state, {sink}), Exception);

    ProcessorState & sink_state = graph.getState(*sink);
    EXPECT_EQ(&source_state, &source->output().getUpdateChannel().getOwner());
    EXPECT_EQ(&sink_state, &sink->input().getUpdateChannel().getOwner());

    IProcessor::UpdatedInputPorts hint_inputs;
    IProcessor::UpdatedOutputPorts hint_outputs;

    source_state.incoming_updates.drain(hint_inputs, hint_outputs);
    EXPECT_TRUE(hint_inputs.empty());
    EXPECT_EQ(std::vector<OutputPort *>{&source->output()}, hint_outputs);

    sink_state.incoming_updates.drain(hint_inputs, hint_outputs);
    EXPECT_EQ(std::vector<InputPort *>{&sink->input()}, hint_inputs);
    EXPECT_TRUE(hint_outputs.empty());
}

TEST(ExecutionGraph, RemoveNeedsAFinishedProcessorAndDisconnectsIt)
{
    auto source = std::make_shared<Source>();
    auto sink = std::make_shared<Sink>();
    connect(source->output(), sink->input());
    auto processors = std::make_shared<Processors>(Processors{source, sink});
    ExecutionGraph graph(processors);

    EXPECT_THROW(graph.remove(source), Exception);

    disconnect(source->output(), sink->input());
    graph.getState(*source).last_status = IProcessor::Status::Finished;
    graph.remove(source);

    EXPECT_EQ((Processors{sink}), *processors);
    EXPECT_FALSE(source->output().getUpdateChannel().isConnected());
    EXPECT_THROW(graph.getState(*source), Exception);
    EXPECT_THROW(graph.remove(source), Exception);
}
