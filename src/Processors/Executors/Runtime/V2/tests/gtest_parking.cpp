#include <Processors/Executors/Runtime/V2/tests/Toolkit/RunPipeline.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Scripted.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Topology.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Thread.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Gate.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>

using namespace DB;
using namespace DB::Runtime;
using namespace DB::Runtime::V2;

namespace
{

VectorWithMemoryTracking<Action> emitsAfterIdleWork(UInt64 first_value, size_t chunks, size_t max_idle_work)
{
    VectorWithMemoryTracking<Action> actions;
    for (size_t chunk = 0; chunk < chunks; ++chunk)
    {
        const size_t idle_work = Wyrd::randomNumberBetween(0, max_idle_work);
        for (size_t step = 0; step < idle_work; ++step)
            actions.push_back(work([](auto &) {}));
        actions.push_back(emit(first_value + chunk));
    }
    return actions;
}

VectorWithMemoryTracking<UInt64> sorted(VectorWithMemoryTracking<UInt64> values)
{
    std::ranges::sort(values);
    return values;
}

}

WYRD_TEST(Parking, BurstsAndPausesFinishEveryChunk, .runs = 1000, .simulation = {.max_steps = 2'000'000})
{
    const size_t sources_count = Wyrd::randomNumberBetween(1, 6);
    const size_t threads = Wyrd::randomNumberBetween(1, 4);
    const size_t max_idle_work = Wyrd::randomNumberBetween(0, 6);

    VectorWithMemoryTracking<ProcessorPtr> sources;
    VectorWithMemoryTracking<UInt64> expected;
    for (size_t index = 0; index < sources_count; ++index)
    {
        const size_t chunks = Wyrd::randomNumberBetween(1, 5);
        const UInt64 first_value = 100 * (index + 1);
        sources.push_back(makeSource(fmt::format("source{}", index), emitsAfterIdleWork(first_value, chunks, max_idle_work)));
        for (size_t chunk = 0; chunk < chunks; ++chunk)
            expected.push_back(first_value + chunk);
    }

    auto sink = makeSink("sink", {});
    auto resize = fanIn(sources, sink);

    Processors processors(sources.begin(), sources.end());
    processors.push_back(resize);
    processors.push_back(sink);

    PoolMetrics metrics;
    EXPECT_NO_THROW(metrics = runPipeline(std::move(processors), threads));
    EXPECT_EQ(sorted(received(*sink)), expected);

    if (threads == 1)
    {
        EXPECT_EQ(metrics.workers.steal_picks, 0u);
        EXPECT_EQ(metrics.workers.parks, 0u);
    }
}

WYRD_TEST(Parking, WorkReleasedWhileOthersSleepWakesThem, .runs = 1000, .simulation = {.max_steps = 2'000'000})
{
    const size_t threads = Wyrd::randomNumberBetween(2, 4);
    Wyrd::Gate gate;

    auto held = makeSource("held", {emit(1), work([&gate](auto &) { gate.pass(); }), emit(2), emit(3)});
    auto transform = makeTransform("transform", {});
    auto sink = makeSink("sink", {});
    chain({held, transform, sink});

    Wyrd::Thread opener([&gate] { gate.open(); });
    EXPECT_NO_THROW(runPipeline({held, transform, sink}, threads));
    opener.join();

    EXPECT_EQ(received(*sink), (VectorWithMemoryTracking<UInt64>{1, 2, 3}));
}

WYRD_TEST(Parking, IdleWorkersAreStoppedAtTheEnd, .runs = 1000)
{
    auto source = makeSource("source", {emit(1)});
    auto sink = makeSink("sink", {});
    chain({source, sink});

    PoolMetrics metrics;
    EXPECT_NO_THROW(metrics = runPipeline({source, sink}, 4));
    EXPECT_EQ(received(*sink), (VectorWithMemoryTracking<UInt64>{1}));

    EXPECT_EQ(metrics.workers.steal_picks, 0u);
    EXPECT_EQ(metrics.workers.parks, 0u);
}
