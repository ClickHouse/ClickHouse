#include <Processors/Executors/Runtime/benchmarks/Shapes.h>
#include <Processors/Executors/Runtime/V1/PipelineExecutor.h>
#include <Processors/Executors/Runtime/V2/PipelineExecutor.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <Common/ThreadPool.h>
#include <Common/VectorWithMemoryTracking.h>

#include <benchmark/benchmark.h>
#include <fmt/format.h>

#include <functional>
#include <memory>
#include <string>

#include <sys/resource.h>

using namespace DB;
using namespace DB::Runtime::Bench;

namespace
{

enum class Executor
{
    V1,
    V2,
};

using Build = std::function<std::shared_ptr<Processors>(ContextPtr context, size_t threads)>;

struct Shape
{
    std::string name;
    Build build;
};

double contextSwitches()
{
    rusage current{};
    getrusage(RUSAGE_SELF, &current);
    return double(current.ru_nvcsw + current.ru_nivcsw);
}

void addMetrics(Runtime::V2::PoolMetrics & total, const IExecutor & executor)
{
    const auto * v2 = dynamic_cast<const Runtime::V2::PipelineExecutor *>(&executor);
    if (!v2)
        return;

    const auto run = v2->getMetrics();
    addCounters(total, run.workers);
    addCounters(total, run.poller);
}

void reportMetrics(benchmark::State & state, const Runtime::V2::PoolMetrics & total)
{
    const auto average = [&](UInt64 value) { return benchmark::Counter(double(value), benchmark::Counter::kAvgIterations); };
    state.counters["async_picks"] = average(total.workers.async_picks);
    state.counters["local_picks"] = average(total.workers.local_picks);
    state.counters["global_picks"] = average(total.workers.global_picks);
    state.counters["steal_picks"] = average(total.workers.steal_picks);
    state.counters["stolen_records"] = average(total.workers.stolen_records);
    state.counters["steal_misses"] = average(total.workers.steal_misses);
    state.counters["offloads"] = average(total.workers.offloads);
    state.counters["parks"] = average(total.workers.parks);
    state.counters["wakes"] = average(total.workers.wakes);
    state.counters["wake_skips"] = average(total.workers.wake_skips);
    state.counters["spawns"] = average(total.workers.spawns);
    state.counters["no_op_wakes"] = average(total.workers.no_op_wakes);
    state.counters["polls"] = average(total.poller.polls);
    state.counters["fired"] = average(total.poller.fired);
    state.counters["poller_wakes"] = average(total.poller.wakes);
}

ExecutorPtr makeExecutor(Executor executor, std::shared_ptr<Processors> processors)
{
    switch (executor)
    {
        case Executor::V1:
            return std::make_shared<Runtime::V1::PipelineExecutor>(std::move(processors), nullptr);
        case Executor::V2:
            return std::make_shared<Runtime::V2::PipelineExecutor>(std::move(processors), nullptr);
    }
}

ContextMutablePtr global_context;

void runShape(benchmark::State & state, Executor executor, const Build & build)
{
    const size_t threads = state.range(0);
    double switches = 0;
    Runtime::V2::PoolMetrics metrics;

    for (auto _ : state)
    {
        state.PauseTiming();
        auto processors = build(global_context, threads);
        state.ResumeTiming();

        const double switches_before = contextSwitches();
        auto pipeline_executor = makeExecutor(executor, processors);
        pipeline_executor->execute(threads, false);
        switches += contextSwitches() - switches_before;

        addMetrics(metrics, *pipeline_executor);
    }

    state.counters["switches"] = benchmark::Counter(switches, benchmark::Counter::kAvgIterations);
    if (executor == Executor::V2)
        reportMetrics(state, metrics);
}

const VectorWithMemoryTracking<Shape> shapes
{
    {"Chain", [](ContextPtr context, size_t) { return chain(context, Load{}); }},
    {"FanOut", [](ContextPtr context, size_t threads) { return fanOut(context, threads, Load{.chunks = 40 * threads}); }},
    {"FanIn", [](ContextPtr context, size_t threads) { return fanIn(context, threads, Load{.chunks = 40 * threads}); }},
    {"Hub", [](ContextPtr context, size_t threads) { return hub(context, threads, Load{.chunks = 30}); }},
    {"ShortQuery", [](ContextPtr context, size_t) { return chain(context, Load{.chunks = 5}); }},
    {"FanInMerge", [](ContextPtr context, size_t threads) { return fanInMerge(context, 4 * threads, Load{.chunks = 40 * 4 * threads, .rows_per_chunk = 8192}); }},
    {"ManySources", [](ContextPtr context, size_t threads) { return manySources(context, 1000, threads, Load{.chunks = 1000, .rows_per_chunk = 8192}); }},
    {"WideFanIn", [](ContextPtr context, size_t) { return fanIn(context, 64, Load{.chunks = 40 * 64}); }},
    {"Delayed", [](ContextPtr context, size_t) { return delayed(context, Load{}); }},
    {"FanInAggregate", [](ContextPtr context, size_t threads) { return fanInAggregate(context, threads, Load{}); }},
    {"Aggregate", [](ContextPtr context, size_t threads) { return aggregate(context, threads, Load{.chunks = 40 * threads}); }},
    {"ManyPartsInOrder", [](ContextPtr context, size_t threads) { return manyPartsInOrder(context, 1000, threads, Load{.rows_per_chunk = 8192}); }},
};

const VectorWithMemoryTracking<std::pair<std::string, Executor>> executors
{
    {"v1", Executor::V1},
    {"v2", Executor::V2},
};

void registerAll()
{
    for (const auto & shape : shapes)
        for (const auto & [version, executor] : executors)
            benchmark::RegisterBenchmark(fmt::format("{}/{}", shape.name, version), runShape, executor, shape.build)
                ->ArgName("threads")
                ->Arg(1)
                ->Arg(4)
                ->Arg(16)
                ->UseRealTime()
                ->MeasureProcessCPUTime()
                ->Unit(benchmark::kMillisecond);
}

}

int main(int argc, char ** argv)
{
    SharedContextHolder shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    global_context->setPath("./");
    registerFunctions();
    GlobalThreadPool::initialize();

    registerAll();
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();

    global_context->shutdown();
    global_context.reset();
    shared_context.reset();
    return 0;
}
