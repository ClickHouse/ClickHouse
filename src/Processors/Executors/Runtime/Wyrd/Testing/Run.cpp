#include <Processors/Executors/Runtime/Wyrd/Testing/Run.h>

#include <base/types.h>

#include <cstdlib>
#include <string>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Simulation/Simulator.h>
#    include <Common/randomSeed.h>
#    include <fmt/format.h>
#    include <magic_enum.hpp>
#    include <stdexcept>
#    include <utility>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Adversary/Access.h>
#    include <base/scope_guard.h>
#    include <fmt/format.h>
#    include <chrono>
#    include <semaphore>
#    include <thread>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION)

static std::pair<UInt64, size_t> seedsFromEnvironment()
{
    if (const char * seed = std::getenv("WYRD_SEED")) // NOLINT(concurrency-mt-unsafe)
        return {std::stoull(seed), 1};

    const char * count = std::getenv("WYRD_SEEDS"); // NOLINT(concurrency-mt-unsafe)
    return {randomSeed(), count ? std::stoull(count) : 10};
}

void runTest(std::function<void()> body)
{
    auto [base_seed, seeds] = seedsFromEnvironment();

    for (size_t i = 0; i < seeds; ++i)
    {
        SimulatorParams params;
        params.seed = base_seed + i;

        SimulatorResult first = Simulator(params, {}).run(body);
        SimulatorResult second = Simulator(params, {}).run(body);

        if (first.digest != second.digest)
            throw std::runtime_error(fmt::format(
                "Simulation with seed {} is not deterministic: digests {} and {}, {} and {} steps",
                params.seed, first.digest, second.digest, first.total_steps, second.total_steps));

        if (first.status != SimulatorResult::Status::Ok)
            throw std::runtime_error(fmt::format(
                "Simulation with seed {} ended with {} after {} steps. {}\nLast syscalls:\n{}",
                params.seed, magic_enum::enum_name(first.status), first.total_steps, first.exception_message, first.last_syscalls));
    }
}

#elif defined(WYRD_ADVERSARY)

static std::chrono::seconds watchdogTimeoutFromEnvironment()
{
    const char * seconds = std::getenv("WYRD_WATCHDOG_SECONDS"); // NOLINT(concurrency-mt-unsafe)
    return std::chrono::seconds(seconds ? std::stoull(seconds) : 60);
}

void runTest(std::function<void()> body)
{
    if (auto * current = adversary())
        current->reset();

    std::chrono::seconds timeout = watchdogTimeoutFromEnvironment();
    std::binary_semaphore finished{0};
    std::thread watchdog(
        [&]
        {
            if (!finished.try_acquire_for(timeout))
            {
                fmt::print(stderr, "Wyrd: the test body did not finish within {} seconds, aborting\n", timeout.count());
                std::abort();
            }
        });

    SCOPE_EXIT({
        finished.release();
        watchdog.join();
    });

    body();

    if (auto * current = adversary())
    {
        AdversaryResult result = current->report();
        fmt::print(stderr, "Wyrd adversary: yields={} suspends={} spurious_failures={}\n", result.yields, result.suspends, result.spurious_failures);
    }
}

#else

void runTest(std::function<void()> body)
{
    body();
}

#endif

}
