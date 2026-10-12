#include <Processors/Executors/Runtime/V2/tests/Toolkit/RunPipeline.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Scripted.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Topology.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>
#include <stdexcept>

using namespace DB;
using namespace DB::Runtime;
using namespace DB::Runtime::V2;

WYRD_TEST(ScriptedPipeline, ChainDeliversEveryChunkInOrder, .runs = 1000, .simulation = { .preempt_probability = 0.5 })
{
    auto source = makeSource("source", {emit(1), emit(2), emit(3)});
    auto transform = makeTransform("transform", {});
    auto sink = makeSink("sink", {});
    chain({source, transform, sink});

    const size_t threads = 1 + Wyrd::randomNumber() % 4;
    PoolMetrics metrics;
    EXPECT_NO_THROW(metrics = runPipeline({source, transform, sink}, threads));
    EXPECT_EQ(received(*sink), (VectorWithMemoryTracking<UInt64>{1, 2, 3}));

    if (threads == 1)
    {
        EXPECT_EQ(metrics.workers.steal_picks, 0u);
        EXPECT_EQ(metrics.workers.parks, 0u);
    }
}

WYRD_TEST(ScriptedPipeline, ThrowInWorkIsRethrownByExecute, .runs = 1000, .simulation = { .preempt_probability = 0.5 })
{
    auto source = makeSource("source", {emit(1), work([](auto &) { throw std::runtime_error("scripted failure"); })});
    auto sink = makeSink("sink", {});
    chain({source, sink});

    const size_t threads = 1 + Wyrd::randomNumber() % 4;
    EXPECT_THROW(runPipeline({source, sink}, threads), std::runtime_error);
}

WYRD_TEST(ScriptedPipeline, FanInCollectsBothSources, .runs = 1000, .simulation = { .preempt_probability = 0.5 })
{
    auto first = makeSource("first", {emit(1)});
    auto second = makeSource("second", {emit(2)});
    auto sink = makeSink("sink", {});
    auto resize = fanIn({first, second}, sink);

    const size_t threads = 1 + Wyrd::randomNumber() % 4;
    PoolMetrics metrics;
    EXPECT_NO_THROW(metrics = runPipeline({first, second, resize, sink}, threads));

    auto values = received(*sink);
    std::ranges::sort(values);
    EXPECT_EQ(values, (VectorWithMemoryTracking<UInt64>{1, 2}));

    if (threads == 1)
    {
        EXPECT_EQ(metrics.workers.steal_picks, 0u);
        EXPECT_EQ(metrics.workers.parks, 0u);
    }
}

WYRD_TEST(ScriptedPipeline, FanOutDeliversEveryChunkOnce, .runs = 1000, .simulation = { .preempt_probability = 0.5 })
{
    auto source = makeSource("source", {emit(1), emit(2), emit(3)});
    auto first = makeSink("first", {});
    auto second = makeSink("second", {});
    auto resize = fanOut(source, {first, second});

    const size_t threads = 1 + Wyrd::randomNumber() % 4;
    PoolMetrics metrics;
    EXPECT_NO_THROW(metrics = runPipeline({source, resize, first, second}, threads));

    auto values = received(*first);
    auto rest = received(*second);
    values.insert(values.end(), rest.begin(), rest.end());
    std::ranges::sort(values);
    EXPECT_EQ(values, (VectorWithMemoryTracking<UInt64>{1, 2, 3}));

    if (threads == 1)
    {
        EXPECT_EQ(metrics.workers.steal_picks, 0u);
        EXPECT_EQ(metrics.workers.parks, 0u);
    }
}
