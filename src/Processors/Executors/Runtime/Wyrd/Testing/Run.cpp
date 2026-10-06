#include <Processors/Executors/Runtime/Wyrd/Testing/Run.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Simulation/Simulation.h>
#    include <Common/randomSeed.h>

#    include <fmt/format.h>
#    include <gtest/gtest.h>
#    include <magic_enum.hpp>

#    include <algorithm>
#    include <charconv>
#    include <cstdlib>
#    include <limits>
#    include <set>
#    include <stdexcept>
#    include <string_view>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Adversary/Access.h>
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

    std::string_view text(value);
    UInt64 seed = 0;
    if (std::from_chars(text.data(), text.data() + text.size(), seed).ec != std::errc{})
        throw std::runtime_error(fmt::format("WYRD_SEED must be a number, got '{}'", text));
    return seed;
}

static bool timelineRequested()
{
    const char * value = std::getenv("WYRD_TIMELINE");
    return value && std::string_view(value) != "0";
}

void runTest(std::function<void()> body, TestParams params)
{
    std::vector<SimulationResult> results;
    const std::optional<UInt64> fixed_seed = params.seed ? params.seed : seedFromEnvironment();
    const size_t runs = fixed_seed ? 1 : params.runs;

    for (size_t i = 0; i < runs; ++i)
    {
        SimulationParams simulation = params.simulation;
        simulation.seed = fixed_seed ? *fixed_seed : randomSeed();

        SimulationResult first = Simulation(simulation).run(body);
        SimulationResult second = Simulation(simulation).run(body);

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
        else if (testing::Test::HasFailure())
        {
            fmt::print(stderr, "Wyrd simulation: run {} failed with seed {} after {} steps; replay with WYRD_SEED={}, add WYRD_TIMELINE=1 for the trace\n", i, simulation.seed, first.total_steps, simulation.seed);
            if (timelineRequested())
                fmt::print(stderr, "Timeline:\n{}\n", first.timeline);
            return;
        }

        results.push_back(std::move(first));
    }

    size_t min_steps = std::numeric_limits<size_t>::max();
    size_t max_steps = 0;
    std::set<UInt64> digests;
    for (const auto & result : results)
    {
        min_steps = std::min(min_steps, result.total_steps);
        max_steps = std::max(max_steps, result.total_steps);
        digests.insert(result.digest);
    }

    fmt::print(stderr, "Wyrd simulation: runs={} steps={}..{} distinct_schedules={}\n", results.size(), min_steps, max_steps, digests.size());
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
            if (!finished.try_acquire_for(params.watchdog_timeout))
            {
                fmt::print(stderr, "Wyrd: the test body did not finish within {} seconds, aborting\n", params.watchdog_timeout.count());
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
