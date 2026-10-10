#include <Processors/Executors/Runtime/V2/tests/Toolkit/RunPipeline.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Scripted.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Topology.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Processors/Port.h>

#include <algorithm>
#include <vector>

using namespace DB;
using namespace DB::Runtime;
using namespace DB::Runtime::V2;

namespace
{

std::vector<UInt64> values(UInt64 first_value, size_t count)
{
    std::vector<UInt64> result;
    for (size_t index = 0; index < count; ++index)
        result.push_back(first_value + index);
    return result;
}

std::vector<UInt64> sorted(std::vector<UInt64> result)
{
    std::ranges::sort(result);
    return result;
}

std::vector<UInt64> receivedByAll(const std::vector<ProcessorPtr> & sinks)
{
    std::vector<UInt64> result;
    for (const auto & sink : sinks)
        result.append_range(received(*sink));
    return result;
}

}

WYRD_TEST(Topology, ChainOfTransformsKeepsTheOrder, .runs = 300)
{
    const size_t chunks = Wyrd::randomNumberBetween(1, 8);
    const size_t transforms = Wyrd::randomNumberBetween(0, 5);

    std::vector<ProcessorPtr> stages{makeSource("source", emits(1, chunks))};
    for (size_t index = 0; index < transforms; ++index)
        stages.push_back(makeTransform(fmt::format("transform{}", index), {}));
    auto sink = makeSink("sink", {});
    stages.push_back(sink);
    chain(stages);

    EXPECT_NO_THROW(runPipeline(Processors(stages.begin(), stages.end()), Wyrd::randomNumberBetween(1, 4)));
    EXPECT_EQ(received(*sink), values(1, chunks));
}

WYRD_TEST(Topology, FanInGathersEverySource, .runs = 300)
{
    const size_t sources_count = Wyrd::randomNumberBetween(1, 6);

    std::vector<ProcessorPtr> sources;
    std::vector<UInt64> expected;
    for (size_t index = 0; index < sources_count; ++index)
    {
        const size_t chunks = Wyrd::randomNumberBetween(1, 4);
        sources.push_back(makeSource(fmt::format("source{}", index), emits(100 * (index + 1), chunks)));
        expected.append_range(values(100 * (index + 1), chunks));
    }
    auto sink = makeSink("sink", {});
    auto resize = fanIn(sources, sink);

    Processors processors(sources.begin(), sources.end());
    processors.push_back(resize);
    processors.push_back(sink);

    EXPECT_NO_THROW(runPipeline(std::move(processors), Wyrd::randomNumberBetween(1, 4)));
    EXPECT_EQ(sorted(received(*sink)), expected);
}

WYRD_TEST(Topology, FanOutDeliversEachChunkToOneSink, .runs = 300)
{
    const size_t chunks = Wyrd::randomNumberBetween(1, 8);
    const size_t sinks_count = Wyrd::randomNumberBetween(1, 6);

    std::vector<ProcessorPtr> sinks;
    for (size_t index = 0; index < sinks_count; ++index)
        sinks.push_back(makeSink(fmt::format("sink{}", index), {}));

    auto source = makeSource("source", emits(1, chunks));
    auto resize = fanOut(source, sinks);

    Processors processors{source, resize};
    processors.append_range(sinks);

    EXPECT_NO_THROW(runPipeline(std::move(processors), Wyrd::randomNumberBetween(1, 4)));
    EXPECT_EQ(sorted(receivedByAll(sinks)), values(1, chunks));
}

WYRD_TEST(Topology, FanInThroughATransformIntoFanOut, .runs = 300)
{
    const size_t sources_count = Wyrd::randomNumberBetween(1, 4);
    const size_t sinks_count = Wyrd::randomNumberBetween(1, 4);

    std::vector<ProcessorPtr> sources;
    std::vector<UInt64> expected;
    for (size_t index = 0; index < sources_count; ++index)
    {
        const size_t chunks = Wyrd::randomNumberBetween(1, 4);
        sources.push_back(makeSource(fmt::format("source{}", index), emits(100 * (index + 1), chunks)));
        expected.append_range(values(100 * (index + 1), chunks));
    }
    std::vector<ProcessorPtr> sinks;
    for (size_t index = 0; index < sinks_count; ++index)
        sinks.push_back(makeSink(fmt::format("sink{}", index), {}));

    auto middle = makeTransform("middle", {});
    auto join = fanIn(sources, middle);
    auto split = fanOut(middle, sinks);

    Processors processors(sources.begin(), sources.end());
    processors.push_back(join);
    processors.push_back(middle);
    processors.push_back(split);
    processors.append_range(sinks);

    EXPECT_NO_THROW(runPipeline(std::move(processors), Wyrd::randomNumberBetween(1, 4)));
    EXPECT_EQ(sorted(receivedByAll(sinks)), expected);
}
