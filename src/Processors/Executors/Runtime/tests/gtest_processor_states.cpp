#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/Runtime/Pipeline/ProcessorStates.h>
#include <Processors/Port.h>

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

/// Grows its inputs from outside, like a merging transform fed by spills.
class Collector final : public IProcessor
{
public:
    Collector() : IProcessor({}, {}) {}
    String getName() const override { return "Collector"; }
    Status prepare() override { return Status::Finished; }
    InputPort & addInput() { return inputs.emplace_back(Block(*makeHeader()), this); }
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

TEST(ProcessorStates, UpdateWiresTheGroupAndTheRequester)
{
    auto source = std::make_shared<Source>();
    auto processors = std::make_shared<Processors>(Processors{source});
    ProcessorStates states(processors);
    ProcessorState & source_state = states.get(*source);
    EXPECT_FALSE(source->output().getUpdateChannel().isConnected());

    auto sink = std::make_shared<Sink>();
    connect(source->output(), sink->input());
    auto updated = states.update(source_state, {sink}, {});
    EXPECT_EQ((Processors{source, sink}), *processors);

    ProcessorState & sink_state = states.get(*sink);
    EXPECT_EQ((std::vector<ProcessorState *>{&sink_state}), updated);
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

TEST(ProcessorStates, UpdateWiresNewPortsOfAListedProcessor)
{
    auto requester = std::make_shared<Source>();
    auto collector = std::make_shared<Collector>();
    auto processors = std::make_shared<Processors>(Processors{requester, collector});
    ProcessorStates states(processors);
    ProcessorState & requester_state = states.get(*requester);
    ProcessorState & collector_state = states.get(*collector);

    auto spill = std::make_shared<Source>();
    InputPort & new_input = collector->addInput();
    connect(spill->output(), new_input);

    auto updated = states.update(requester_state, {spill}, {collector});
    EXPECT_EQ((std::vector<ProcessorState *>{&collector_state, &states.get(*spill)}), updated);
    EXPECT_EQ(&collector_state, &new_input.getUpdateChannel().getOwner());
    EXPECT_EQ(&states.get(*spill), &spill->output().getUpdateChannel().getOwner());

    IProcessor::UpdatedInputPorts hint_inputs;
    IProcessor::UpdatedOutputPorts hint_outputs;
    collector_state.incoming_updates.drain(hint_inputs, hint_outputs);
    EXPECT_EQ(std::vector<InputPort *>{&new_input}, hint_inputs);
    EXPECT_TRUE(hint_outputs.empty());

    states.update(requester_state, {}, {collector});
    collector_state.incoming_updates.drain(hint_inputs, hint_outputs);
    EXPECT_TRUE(hint_inputs.empty());
    EXPECT_TRUE(hint_outputs.empty());
}

TEST(ProcessorStates, RemoveDisconnectsTheFinishedProcessors)
{
    Chain chain;
    ProcessorStates states(chain.processors);

    states.get(*chain.source).last_status = IProcessor::Status::Finished;
    states.get(*chain.sink).last_status = IProcessor::Status::Finished;
    states.remove({chain.source, chain.sink});

    EXPECT_TRUE(chain.processors->empty());
    EXPECT_FALSE(chain.source->output().getUpdateChannel().isConnected());
}
