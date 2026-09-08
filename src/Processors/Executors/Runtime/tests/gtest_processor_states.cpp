#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/Runtime/Pipeline/ProcessorStates.h>
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

struct Chain
{
    std::shared_ptr<Source> source = std::make_shared<Source>();
    std::shared_ptr<Sink> sink = std::make_shared<Sink>();
    std::shared_ptr<Processors> processors;

    Chain()
    {
        connect(source->output(), sink->input());
        processors = std::make_shared<Processors>(Processors{source, sink});
    }
};

std::vector<IProcessor *> visited(ProcessorStates & states)
{
    std::vector<IProcessor *> result;
    states.forEachProcessor([&](IProcessor & processor, ProcessorState & state)
    {
        EXPECT_EQ(&processor, state.processor);
        result.push_back(&processor);
    });
    return result;
}

}

TEST(ProcessorStates, WiresBothEndsAndKeepsTheSharedList)
{
    Chain chain;
    ProcessorStates states(chain.processors);

    EXPECT_EQ((std::vector<IProcessor *>{chain.source.get(), chain.sink.get()}), visited(states));

    ProcessorState & source_state = states.get(*chain.source);
    ProcessorState & sink_state = states.get(*chain.sink);
    EXPECT_EQ(chain.source.get(), source_state.processor);
    EXPECT_EQ(&source_state, &chain.source->output().getUpdateChannel().getOwner());
    EXPECT_EQ(&sink_state, &chain.sink->input().getUpdateChannel().getOwner());

    EXPECT_FALSE(states.dump().empty());
}

TEST(ProcessorStates, NeighbourOutsideTheListIsLogicalError)
{
    auto source = std::make_shared<Source>();
    auto omitted_sink = std::make_shared<Sink>();
    connect(source->output(), omitted_sink->input());

    auto processors = std::make_shared<Processors>(Processors{source});
    try
    {
        ProcessorStates states(processors);
        FAIL() << "expected a LOGICAL_ERROR";
    }
    catch (const Exception & e)
    {
        EXPECT_NE(std::string::npos, e.message().find("was found as output for processor"));
    }
}

TEST(ProcessorStates, AddWiresTheGroupAndTheRequester)
{
    auto source = std::make_shared<Source>();
    auto processors = std::make_shared<Processors>(Processors{source});
    ProcessorStates states(processors);
    ProcessorState & source_state = states.get(*source);
    EXPECT_FALSE(source->output().getUpdateChannel().isConnected());

    auto sink = std::make_shared<Sink>();
    connect(source->output(), sink->input());
    auto added = states.add(source_state, {sink});
    ASSERT_EQ(1u, added.size());
    EXPECT_EQ(sink.get(), added.front()->processor);

    EXPECT_EQ((Processors{source, sink}), *processors);
    EXPECT_THROW(states.add(source_state, {sink}), Exception);

    ProcessorState & sink_state = states.get(*sink);
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

TEST(ProcessorStates, RemoveNeedsFinishedProcessorsAndDisconnectsThem)
{
    Chain chain;
    ProcessorStates states(chain.processors);

    EXPECT_THROW(states.remove({chain.source, chain.sink}), Exception);
    EXPECT_EQ(2u, chain.processors->size());

    states.get(*chain.source).last_status = IProcessor::Status::Finished;
    states.get(*chain.sink).last_status = IProcessor::Status::Finished;
    states.remove({chain.source, chain.sink});

    EXPECT_TRUE(chain.processors->empty());
    EXPECT_FALSE(chain.source->output().getUpdateChannel().isConnected());
    EXPECT_THROW(states.get(*chain.source), Exception);
}
