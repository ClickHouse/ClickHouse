#include <Processors/Executors/Runtime/Topology/ExecutionGraph.h>
#include <Processors/IProcessor.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <vector>

using namespace DB;

namespace
{

class DummyProcessor final : public IProcessor
{
public:
    String getName() const override { return "Dummy"; }
    Status prepare() override { return Status::Finished; }
};

std::vector<IProcessor *> visited(ExecutionGraph & graph)
{
    std::vector<IProcessor *> result;
    graph.forEachProcessor([&](IProcessor & processor) { result.push_back(&processor); });
    return result;
}

}

TEST(ExecutionGraph, SharesTheListWithTheOwner)
{
    auto first = std::make_shared<DummyProcessor>();
    auto second = std::make_shared<DummyProcessor>();
    auto third = std::make_shared<DummyProcessor>();

    auto processors = std::make_shared<Processors>(Processors{first, second});
    ExecutionGraph graph(processors);

    EXPECT_EQ((std::vector<IProcessor *>{first.get(), second.get()}), visited(graph));

    graph.addProcessor(third);
    EXPECT_EQ((Processors{first, second, third}), *processors);

    graph.removeProcessor(second);
    EXPECT_EQ((Processors{first, third}), *processors);
    EXPECT_EQ((std::vector<IProcessor *>{first.get(), third.get()}), visited(graph));

    EXPECT_THROW(graph.removeProcessor(second), Exception);
}

TEST(ExecutionGraph, EmptyPipeline)
{
    auto processors = std::make_shared<Processors>();
    ExecutionGraph graph(processors);

    EXPECT_TRUE(visited(graph).empty());
}
