#include <Processors/Executors/Runtime/V2/tests/Toolkit/RunPipeline.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Scripted.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Topology.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Processors/Port.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>

using namespace DB;
using namespace DB::Runtime;
using namespace DB::Runtime::V2;

namespace
{

VectorWithMemoryTracking<UInt64> values(UInt64 first_value, size_t count)
{
    VectorWithMemoryTracking<UInt64> result;
    for (size_t index = 0; index < count; ++index)
        result.push_back(first_value + index);
    return result;
}

VectorWithMemoryTracking<UInt64> sorted(VectorWithMemoryTracking<UInt64> result)
{
    std::ranges::sort(result);
    return result;
}

VectorWithMemoryTracking<UInt64> receivedByAll(const VectorWithMemoryTracking<ProcessorPtr> & sinks)
{
    VectorWithMemoryTracking<UInt64> result;
    for (const auto & sink : sinks)
        result.append_range(received(*sink));
    return result;
}

}

WYRD_TEST(Topology, ChainOfTransformsKeepsTheOrder, .runs = 300)
{
    const size_t chunks = Wyrd::randomNumberBetween(1, 8);
    const size_t transforms = Wyrd::randomNumberBetween(0, 5);

    VectorWithMemoryTracking<ProcessorPtr> stages{makeSource("source", emits(1, chunks))};
    for (size_t index = 0; index < transforms; ++index)
        stages.push_back(makeTransform(fmt::format("transform{}", index), {}));
    auto sink = makeSink("sink", {});
    stages.push_back(sink);
    chain(stages);

    const size_t threads = Wyrd::randomNumberBetween(1, 4);
    PoolMetrics metrics;
    EXPECT_NO_THROW(metrics = runPipeline(Processors(stages.begin(), stages.end()), threads));
    EXPECT_EQ(received(*sink), values(1, chunks));

    if (threads == 1)
    {
        EXPECT_EQ(metrics.workers.steal_picks, 0u);
        EXPECT_EQ(metrics.workers.parks, 0u);
    }
}

WYRD_TEST(Topology, FanInGathersEverySource, .runs = 300)
{
    const size_t sources_count = Wyrd::randomNumberBetween(1, 6);

    VectorWithMemoryTracking<ProcessorPtr> sources;
    VectorWithMemoryTracking<UInt64> expected;
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

    const size_t threads = Wyrd::randomNumberBetween(1, 4);
    PoolMetrics metrics;
    EXPECT_NO_THROW(metrics = runPipeline(std::move(processors), threads));
    EXPECT_EQ(sorted(received(*sink)), expected);

    if (threads == 1)
    {
        EXPECT_EQ(metrics.workers.steal_picks, 0u);
        EXPECT_EQ(metrics.workers.parks, 0u);
    }
}

WYRD_TEST(Topology, FanOutDeliversEachChunkToOneSink, .runs = 300)
{
    const size_t chunks = Wyrd::randomNumberBetween(1, 8);
    const size_t sinks_count = Wyrd::randomNumberBetween(1, 6);

    VectorWithMemoryTracking<ProcessorPtr> sinks;
    for (size_t index = 0; index < sinks_count; ++index)
        sinks.push_back(makeSink(fmt::format("sink{}", index), {}));

    auto source = makeSource("source", emits(1, chunks));
    auto resize = fanOut(source, sinks);

    Processors processors{source, resize};
    processors.append_range(sinks);

    const size_t threads = Wyrd::randomNumberBetween(1, 4);
    PoolMetrics metrics;
    EXPECT_NO_THROW(metrics = runPipeline(std::move(processors), threads));
    EXPECT_EQ(sorted(receivedByAll(sinks)), values(1, chunks));

    if (threads == 1)
    {
        EXPECT_EQ(metrics.workers.steal_picks, 0u);
        EXPECT_EQ(metrics.workers.parks, 0u);
    }
}

WYRD_TEST(Topology, FanInThroughATransformIntoFanOut, .runs = 300)
{
    const size_t sources_count = Wyrd::randomNumberBetween(1, 4);
    const size_t sinks_count = Wyrd::randomNumberBetween(1, 4);

    VectorWithMemoryTracking<ProcessorPtr> sources;
    VectorWithMemoryTracking<UInt64> expected;
    for (size_t index = 0; index < sources_count; ++index)
    {
        const size_t chunks = Wyrd::randomNumberBetween(1, 4);
        sources.push_back(makeSource(fmt::format("source{}", index), emits(100 * (index + 1), chunks)));
        expected.append_range(values(100 * (index + 1), chunks));
    }
    VectorWithMemoryTracking<ProcessorPtr> sinks;
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

    const size_t threads = Wyrd::randomNumberBetween(1, 4);
    PoolMetrics metrics;
    EXPECT_NO_THROW(metrics = runPipeline(std::move(processors), threads));
    EXPECT_EQ(sorted(receivedByAll(sinks)), expected);

    if (threads == 1)
    {
        EXPECT_EQ(metrics.workers.steal_picks, 0u);
        EXPECT_EQ(metrics.workers.parks, 0u);
    }
}
