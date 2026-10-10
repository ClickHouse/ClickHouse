#include <Processors/Executors/Runtime/V2/tests/Toolkit/Scripted.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Topology.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Processors/Port.h>

#include <vector>

using namespace DB;
using namespace DB::Runtime;
using namespace DB::Runtime::V2;

namespace
{

const IProcessor & downstreamOf(const IProcessor & processor)
{
    return processor.getOutputs().front().getInputPort().getProcessor();
}

std::vector<ProcessorPtr> ordered(const Processors & processors)
{
    return {processors.begin(), processors.end()};
}

}

WYRD_TEST(Topology, ChainConnectsEachStageToTheNext, .runs = 1000)
{
    auto source = makeSource("source", {});
    auto transform = makeTransform("transform", {});
    auto sink = makeSink("sink", {});

    auto processors = ordered(chain({source, transform, sink}));

    ASSERT_EQ(processors.size(), 3u);
    EXPECT_EQ(processors[0], source);
    EXPECT_EQ(processors[1], transform);
    EXPECT_EQ(processors[2], sink);
    EXPECT_EQ(&downstreamOf(*source), transform.get());
    EXPECT_EQ(&downstreamOf(*transform), sink.get());
    EXPECT_TRUE(sink->getInputs().front().isConnected());
}

WYRD_TEST(Topology, FanInJoinsSourcesThroughOneResize, .runs = 1000)
{
    auto first = makeSource("first", {});
    auto second = makeSource("second", {});
    auto sink = makeSink("sink", {});

    auto processors = ordered(fanIn({first, second}, sink));

    ASSERT_EQ(processors.size(), 4u);
    EXPECT_EQ(processors[0], first);
    EXPECT_EQ(processors[1], second);
    EXPECT_EQ(processors[3], sink);

    const auto & resize = *processors[2];
    EXPECT_EQ(resize.getName(), "Resize");
    EXPECT_EQ(resize.getInputs().size(), 2u);
    EXPECT_EQ(resize.getOutputs().size(), 1u);
    EXPECT_EQ(&downstreamOf(*first), &resize);
    EXPECT_EQ(&downstreamOf(*second), &resize);
    EXPECT_EQ(&downstreamOf(resize), sink.get());
}

WYRD_TEST(Topology, FanOutSplitsOneSourceThroughOneResize, .runs = 1000)
{
    auto source = makeSource("source", {});
    auto first = makeSink("first", {});
    auto second = makeSink("second", {});

    auto processors = ordered(fanOut(source, {first, second}));

    ASSERT_EQ(processors.size(), 4u);
    EXPECT_EQ(processors[0], source);
    EXPECT_EQ(processors[2], first);
    EXPECT_EQ(processors[3], second);

    const auto & resize = *processors[1];
    EXPECT_EQ(resize.getName(), "Resize");
    EXPECT_EQ(resize.getInputs().size(), 1u);
    EXPECT_EQ(resize.getOutputs().size(), 2u);
    EXPECT_EQ(&downstreamOf(*source), &resize);

    std::vector<const IProcessor *> sinks;
    for (const auto & output : resize.getOutputs())
        sinks.push_back(&output.getInputPort().getProcessor());
    EXPECT_EQ(sinks, (std::vector<const IProcessor *>{first.get(), second.get()}));
}
