#include <Processors/Executors/Runtime/V2/tests/Toolkit/RunPipeline.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Scripted.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Topology.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>

#include <algorithm>
#include <stdexcept>

using namespace DB;
using namespace DB::Runtime;
using namespace DB::Runtime::V2;

WYRD_TEST(ScriptedPipeline, ChainDeliversEveryChunkInOrder, .runs = 1000, .simulation = { .max_syncs_before_preempt = 3 })
{
    auto sink = makeSink("sink", {});
    auto processors = chain({makeSource("source", {emit(1), emit(2), emit(3)}), makeTransform("transform", {}), sink});

    const size_t threads = 1 + Wyrd::randomNumber() % 4;
    EXPECT_NO_THROW(runPipeline(std::move(processors), threads));
    EXPECT_EQ(received(*sink), (std::vector<UInt64>{1, 2, 3}));
}

WYRD_TEST(ScriptedPipeline, ThrowInWorkIsRethrownByExecute, .runs = 1000, .simulation = { .max_syncs_before_preempt = 3 })
{
    auto sink = makeSink("sink", {});
    auto processors = chain({makeSource("source", {emit(1), work([](auto &) { throw std::runtime_error("scripted failure"); })}), sink});

    const size_t threads = 1 + Wyrd::randomNumber() % 4;
    EXPECT_THROW(runPipeline(std::move(processors), threads), std::runtime_error);
}

WYRD_TEST(ScriptedPipeline, FanInCollectsBothSources, .runs = 1000, .simulation = { .max_syncs_before_preempt = 3 })
{
    auto sink = makeSink("sink", {});
    auto processors = fanIn({makeSource("first", {emit(1)}), makeSource("second", {emit(2)})}, sink);

    const size_t threads = 1 + Wyrd::randomNumber() % 4;
    EXPECT_NO_THROW(runPipeline(std::move(processors), threads));

    auto values = received(*sink);
    std::ranges::sort(values);
    EXPECT_EQ(values, (std::vector<UInt64>{1, 2}));
}

WYRD_TEST(ScriptedPipeline, FanOutDeliversEveryChunkOnce, .runs = 1000, .simulation = { .max_syncs_before_preempt = 3 })
{
    auto first = makeSink("first", {});
    auto second = makeSink("second", {});
    auto processors = fanOut(makeSource("source", {emit(1), emit(2), emit(3)}), {first, second});

    const size_t threads = 1 + Wyrd::randomNumber() % 4;
    EXPECT_NO_THROW(runPipeline(std::move(processors), threads));

    auto values = received(*first);
    auto rest = received(*second);
    values.insert(values.end(), rest.begin(), rest.end());
    std::ranges::sort(values);
    EXPECT_EQ(values, (std::vector<UInt64>{1, 2, 3}));
}
