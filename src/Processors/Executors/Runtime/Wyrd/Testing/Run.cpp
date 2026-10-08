#include <Processors/Executors/Runtime/Wyrd/Testing/Run.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Simulation.h>
#    include <Common/randomSeed.h>

#    include <fmt/format.h>
#    include <gtest/gtest-spi.h>
#    include <gtest/gtest.h>
#    include <magic_enum.hpp>

#    include <algorithm>
#    include <atomic>
#    include <cstdlib>
#    include <exception>
#    include <optional>
#    include <set>
#    include <stdexcept>
#    include <string>
#    include <string_view>
#    include <thread>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/Access.h>
#    include <base/scope_guard.h>

#    include <fmt/format.h>
#    include <gtest/gtest.h>

#    include <cstdlib>
#    include <semaphore>
#    include <thread>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION)

static std::optional<UInt64> seedFromEnvironment()
{
    const char * value = std::getenv("WYRD_SEED");
    if (!value)
        return std::nullopt;
    return std::stoull(value);
}

static bool timelineRequested()
{
    const char * value = std::getenv("WYRD_TIMELINE");
    return value && std::string_view(value) != "0";
}

static size_t threadsFromEnvironment()
{
    const char * value = std::getenv("WYRD_THREADS");
    if (!value)
        return std::max<size_t>(1, std::thread::hardware_concurrency());
    return std::stoull(value);
}

struct CapturedFailure
{
    std::string file;
    int line;
    std::string message;
};

struct FailedRun
{
    size_t run;
    UInt64 seed;
    size_t steps;
    std::string timeline;
    std::vector<CapturedFailure> failures;
};

struct RunnerOutcome
{
    std::vector<SimulationResult> results;
    std::optional<FailedRun> failure;
    std::exception_ptr error;
};

void runTest(std::function<void()> body, TestParams params)
{
    const std::optional<UInt64> fixed_seed = params.seed ? params.seed : seedFromEnvironment();
    const size_t runs = fixed_seed ? 1 : params.runs;
    const size_t threads = fixed_seed ? 1 : std::max<size_t>(1, std::min(runs, threadsFromEnvironment()));
    const bool timeline_requested = timelineRequested();

    std::atomic<bool> stop{false};
    std::vector<RunnerOutcome> outcomes(threads);

    auto run_simulation = [&](size_t i, RunnerOutcome & outcome)
    {
        SimulationParams simulation = params.simulation;
        simulation.seed = fixed_seed ? *fixed_seed : randomSeed();
        simulation.collect_timeline = timeline_requested;

        testing::TestPartResultArray failures;
        SimulationResult first;
        SimulationResult second;
        {
            testing::ScopedFakeTestPartResultReporter reporter(testing::ScopedFakeTestPartResultReporter::INTERCEPT_ONLY_CURRENT_THREAD, &failures);
            first = Simulation(simulation).run(body);
            second = Simulation(simulation).run(body);
        }

        if (first.digest != second.digest)
        {
            throw std::runtime_error(fmt::format(
                "Simulation with seed {} is not deterministic: digests {} and {}, {} and {} steps",
                simulation.seed, first.digest, second.digest, first.total_steps, second.total_steps));
        }
        else if (first.status != SimulationResult::Status::Ok)
        {
            throw std::runtime_error(fmt::format(
                "Simulation with seed {} ended with {} after {} steps. {}\nTimeline:\n{}",
                simulation.seed,
                magic_enum::enum_name(first.status),
                first.total_steps,
                first.exception_message,
                first.timeline));
        }

        if (failures.size() > 0)
        {
            FailedRun & failed = outcome.failure.emplace(FailedRun{.run = i, .seed = simulation.seed, .steps = first.total_steps, .timeline = std::move(first.timeline), .failures = {}});
            for (int index = 0; index < failures.size(); ++index)
            {
                const auto & failure = failures.GetTestPartResult(index);
                failed.failures.push_back({.file = failure.file_name() ? failure.file_name() : "", .line = failure.line_number(), .message = failure.message()});
            }
            stop.store(true);
            return;
        }

        outcome.results.push_back(std::move(first));
    };

    auto worker = [&](size_t thread)
    {
        RunnerOutcome & outcome = outcomes[thread];
        try
        {
            for (size_t i = thread; i < runs && !stop.load() && !outcome.failure; i += threads)
                run_simulation(i, outcome);
        }
        catch (...)
        {
            outcome.error = std::current_exception();
            stop.store(true);
        }
    };

    std::vector<std::thread> pool;
    for (size_t thread = 0; thread < threads; ++thread)
        pool.emplace_back(worker, thread);
    for (auto & thread : pool)
        thread.join();

    size_t completed = 0;
    size_t min_steps = 0;
    size_t max_steps = 0;
    std::set<UInt64> digests;
    for (const auto & outcome : outcomes)
    {
        for (const auto & result : outcome.results)
        {
            min_steps = completed == 0 ? result.total_steps : std::min(min_steps, result.total_steps);
            max_steps = std::max(max_steps, result.total_steps);
            digests.insert(result.digest);
            ++completed;
        }
    }
    fmt::print(stderr, "Wyrd simulation: runs={} threads={} steps={}..{} distinct_schedules={}\n", completed, threads, min_steps, max_steps, digests.size());

    const auto failed = std::ranges::find_if(outcomes, [](const RunnerOutcome & outcome) { return outcome.failure.has_value(); });
    if (failed != outcomes.end())
    {
        const FailedRun & failure = *failed->failure;
        for (const auto & [file, line, message] : failure.failures)
            ADD_FAILURE_AT(file.c_str(), line) << message;
        fmt::print(stderr, "Wyrd simulation: run {} failed with seed {} after {} steps; replay with WYRD_SEED={}, add WYRD_TIMELINE=1 for the trace\n", failure.run, failure.seed, failure.steps, failure.seed);
        if (timeline_requested)
            fmt::print(stderr, "Timeline:\n{}\n", failure.timeline);
    }

    for (const auto & outcome : outcomes)
        if (outcome.error)
            std::rethrow_exception(outcome.error);
}

#elif defined(WYRD_ADVERSARY)

void runTest(std::function<void()> body, TestParams params)
{
    setAdversary(params.adversary);
    SCOPE_EXIT({ resetAdversary(); });

    std::binary_semaphore finished{0};
    std::thread watchdog(
        [&]
        {
            if (!finished.try_acquire_for(params.adversary.watchdog_timeout))
            {
                fmt::print(stderr, "Wyrd: the test body did not finish within {} seconds, aborting\n", params.adversary.watchdog_timeout.count());
                std::abort();
            }
        });

    SCOPE_EXIT({
        finished.release();
        watchdog.join();
    });

    AdversaryResult total{};
    for (size_t i = 0; i < params.runs; ++i)
    {
        adversary()->reset();
        body();

        AdversaryResult result = adversary()->report();
        total.yields += result.yields;
        total.suspends += result.suspends;
        total.spurious_failures += result.spurious_failures;

        if (testing::Test::HasFailure())
        {
            fmt::print(stderr, "Wyrd adversary: run {} failed; yields={} suspends={} spurious_failures={}\n", i, result.yields, result.suspends, result.spurious_failures);
            return;
        }
    }

    fmt::print(stderr, "Wyrd adversary: runs={} yields={} suspends={} spurious_failures={}\n", params.runs, total.yields, total.suspends, total.spurious_failures);
}

#else

void runTest(std::function<void()> body, TestParams)
{
    body();
}

#endif

}
