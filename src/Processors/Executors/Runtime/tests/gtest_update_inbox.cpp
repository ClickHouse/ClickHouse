#include <Processors/Executors/Runtime/Pipeline/UpdateInbox.h>
#include <Processors/Port.h>

#include <gtest/gtest.h>

#include <atomic>
#include <map>
#include <thread>
#include <vector>

using namespace DB;

namespace
{

InputPorts makeInputs(size_t count)
{
    InputPorts ports;
    for (size_t i = 0; i < count; ++i)
        ports.emplace_back(Block{});
    return ports;
}

std::vector<InputPort *> pointers(InputPorts & ports)
{
    std::vector<InputPort *> result;
    for (auto & port : ports)
        result.push_back(&port);
    return result;
}

}

TEST(UpdateInbox, DrainKeepsPushOrderAndDeduplicates)
{
    auto inputs = makeInputs(3);
    auto ports = pointers(inputs);

    UpdateInbox inbox;
    inbox.push(*ports[0]);
    inbox.push(*ports[1]);
    inbox.push(*ports[0]);
    inbox.push(*ports[2]);
    inbox.push(*ports[1]);

    IProcessor::UpdatedInputPorts drained_inputs;
    IProcessor::UpdatedOutputPorts drained_outputs;
    inbox.drain(drained_inputs, drained_outputs);

    EXPECT_EQ(ports, drained_inputs);
    EXPECT_TRUE(drained_outputs.empty());

    inbox.drain(drained_inputs, drained_outputs);
    EXPECT_TRUE(drained_inputs.empty());

    inbox.push(*ports[1]);
    inbox.drain(drained_inputs, drained_outputs);
    EXPECT_EQ(std::vector<InputPort *>{ports[1]}, drained_inputs);
}

TEST(UpdateInbox, InputsAndOutputsAreSeparate)
{
    InputPort input(Block{});
    OutputPort output(Block{});

    UpdateInbox inbox;
    inbox.push(output);
    inbox.push(input);

    IProcessor::UpdatedInputPorts drained_inputs;
    IProcessor::UpdatedOutputPorts drained_outputs;
    inbox.drain(drained_inputs, drained_outputs);

    EXPECT_EQ(std::vector<InputPort *>{&input}, drained_inputs);
    EXPECT_EQ(std::vector<OutputPort *>{&output}, drained_outputs);
}

TEST(UpdateInbox, ConcurrentPushesKeepPerThreadOrder)
{
    constexpr size_t pushers = 8;
    constexpr size_t ports_per_pusher = 1000;

    std::vector<InputPorts> inputs(pushers);
    std::vector<std::vector<InputPort *>> ports(pushers);
    for (size_t i = 0; i < pushers; ++i)
    {
        inputs[i] = makeInputs(ports_per_pusher);
        ports[i] = pointers(inputs[i]);
    }

    UpdateInbox inbox;

    std::vector<std::thread> threads;
    for (size_t i = 0; i < pushers; ++i)
    {
        threads.emplace_back([&, i]
        {
            for (auto * port : ports[i])
                inbox.push(*port);
        });
    }

    for (auto & thread : threads)
        thread.join();

    IProcessor::UpdatedInputPorts drained_inputs;
    IProcessor::UpdatedOutputPorts drained_outputs;
    inbox.drain(drained_inputs, drained_outputs);

    ASSERT_EQ(pushers * ports_per_pusher, drained_inputs.size());

    std::map<InputPort *, size_t> position;
    for (size_t i = 0; i < drained_inputs.size(); ++i)
        EXPECT_TRUE(position.emplace(drained_inputs[i], i).second);

    for (size_t i = 0; i < pushers; ++i)
        for (size_t j = 1; j < ports_per_pusher; ++j)
            EXPECT_LT(position.at(ports[i][j - 1]), position.at(ports[i][j]));
}

TEST(UpdateInbox, ConcurrentDrainDeliversEveryPortOnce)
{
    constexpr size_t pushers = 4;
    constexpr size_t ports_per_pusher = 5000;

    std::vector<InputPorts> inputs(pushers);
    std::vector<std::vector<InputPort *>> ports(pushers);
    for (size_t i = 0; i < pushers; ++i)
    {
        inputs[i] = makeInputs(ports_per_pusher);
        ports[i] = pointers(inputs[i]);
    }

    UpdateInbox inbox;
    std::atomic<size_t> active_pushers{pushers};
    std::map<InputPort *, size_t> delivered;

    std::thread drainer([&]
    {
        IProcessor::UpdatedInputPorts drained_inputs;
        IProcessor::UpdatedOutputPorts drained_outputs;

        auto collect = [&]
        {
            inbox.drain(drained_inputs, drained_outputs);
            for (auto * port : drained_inputs)
                ++delivered[port];
        };

        while (active_pushers.load() != 0)
            collect();

        collect();
    });

    std::vector<std::thread> threads;
    for (size_t i = 0; i < pushers; ++i)
    {
        threads.emplace_back([&, i]
        {
            for (auto * port : ports[i])
                inbox.push(*port);
            active_pushers.fetch_sub(1);
        });
    }

    for (auto & thread : threads)
        thread.join();
    drainer.join();

    ASSERT_EQ(pushers * ports_per_pusher, delivered.size());
    for (const auto & [port, count] : delivered)
        EXPECT_EQ(1u, count);
}
