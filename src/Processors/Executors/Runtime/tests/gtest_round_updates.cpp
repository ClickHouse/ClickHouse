#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>
#include <Processors/Executors/Runtime/Pipeline/RoundUpdates.h>
#include <Processors/Port.h>

#include <gtest/gtest.h>

#include <memory>

using namespace DB;

TEST(RoundUpdates, ChannelDeduplicatesWithinRound)
{
    ProcessorState state;
    InputPort first(Block{});
    InputPort second(Block{});
    first.getUpdateChannel().connect(state, first);
    second.getUpdateChannel().connect(state, second);

    first.getUpdateChannel().notifyChanges();
    first.getUpdateChannel().notifyChanges();
    second.getUpdateChannel().notifyChanges();
    first.getUpdateChannel().notifyChanges();

    IProcessor::UpdatedInputPorts inputs;
    IProcessor::UpdatedOutputPorts outputs;
    state.round_updates.drain(inputs, outputs);

    EXPECT_EQ((std::vector<InputPort *>{&first, &second}), inputs);
    EXPECT_TRUE(outputs.empty());

    state.round_updates.drain(inputs, outputs);
    EXPECT_TRUE(inputs.empty());

    first.getUpdateChannel().notifyChanges();
    state.round_updates.drain(inputs, outputs);
    EXPECT_EQ(std::vector<InputPort *>{&first}, inputs);
}

TEST(RoundUpdates, ChannelNotifiesUntilDisconnected)
{
    ProcessorState state;
    InputPort input(Block{});
    OutputPort output(Block{});

    EXPECT_FALSE(input.getUpdateChannel().isConnected());
    input.getUpdateChannel().notifyChanges();

    input.getUpdateChannel().connect(state, input);
    output.getUpdateChannel().connect(state, output);
    EXPECT_TRUE(input.getUpdateChannel().isConnected());

    input.getUpdateChannel().notifyChanges();
    output.getUpdateChannel().notifyChanges();
    output.getUpdateChannel().notifyChanges();

    IProcessor::UpdatedInputPorts inputs;
    IProcessor::UpdatedOutputPorts outputs;
    state.round_updates.drain(inputs, outputs);
    EXPECT_EQ(std::vector<InputPort *>{&input}, inputs);
    EXPECT_EQ(std::vector<OutputPort *>{&output}, outputs);

    input.getUpdateChannel().disconnect();
    EXPECT_FALSE(input.getUpdateChannel().isConnected());
    input.getUpdateChannel().notifyChanges();

    state.round_updates.drain(inputs, outputs);
    EXPECT_TRUE(inputs.empty());
    EXPECT_TRUE(outputs.empty());

    output.getUpdateChannel().disconnect();
    output.getUpdateChannel().disconnect();
    EXPECT_FALSE(output.getUpdateChannel().isConnected());
}

TEST(RoundUpdates, CopiedPortHasDisconnectedChannel)
{
    ProcessorState state;
    InputPort input(Block{});

    InputPort copy(input);
    EXPECT_FALSE(copy.getUpdateChannel().isConnected());
}
