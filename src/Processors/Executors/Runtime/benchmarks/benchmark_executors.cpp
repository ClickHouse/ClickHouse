#include <Processors/Executors/Runtime/benchmarks/Shapes.h>
#include <Processors/Executors/Runtime/V1/PipelineExecutor.h>
#include <Processors/Executors/Runtime/V2/PipelineExecutor.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <Common/ThreadPool.h>

#include <benchmark/benchmark.h>
#include <fmt/format.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

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

void addMetrics(Runtime::V2::Metrics & total, const IExecutor & executor)
{
    const auto * v2 = dynamic_cast<const Runtime::V2::PipelineExecutor *>(&executor);
    if (!v2)
        return;

    total = combine(total, v2->getMetrics());
}

void reportMetrics(benchmark::State & state, const Runtime::V2::Metrics & total)
{
    const auto average = [&](UInt64 value) { return benchmark::Counter(double(value), benchmark::Counter::kAvgIterations); };
    state.counters["local_picks"] = average(total.local_picks);
    state.counters["global_picks"] = average(total.global_picks);
    state.counters["poll_picks"] = average(total.poll_picks);
    state.counters["steal_picks"] = average(total.steal_picks);
    state.counters["offloads"] = average(total.offloads);
    state.counters["parks"] = average(total.parks);
    state.counters["steal_misses"] = average(total.steal_misses);
    state.counters["wakes"] = average(total.wakes);
    state.counters["wake_skips"] = average(total.wake_skips);
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
    Runtime::V2::Metrics metrics;

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

const std::vector<Shape> shapes
{
    {"Chain", [](ContextPtr context, size_t) { return chain(context, Load{}); }},
    {"FanOut", [](ContextPtr context, size_t threads) { return fanOut(context, threads, Load{}); }},
    {"FanIn", [](ContextPtr context, size_t threads) { return fanIn(context, threads, Load{}); }},
    {"Hub", [](ContextPtr context, size_t threads) { return hub(context, threads, Load{}); }},
    {"ShortQuery", [](ContextPtr context, size_t) { return chain(context, Load{.chunks = 5}); }},
};

const std::vector<std::pair<std::string, Executor>> executors
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
