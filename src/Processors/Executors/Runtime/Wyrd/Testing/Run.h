#pragma once

#include <Processors/Executors/Runtime/Wyrd/Adversary/AdversaryParams.h>
#include <Processors/Executors/Runtime/Wyrd/Simulation/SimulationParams.h>

#include <chrono>
#include <functional>
#include <optional>

namespace DB::Runtime::Wyrd
{

struct TestParams
{
    size_t runs = 10;
    std::optional<UInt64> seed = std::nullopt;
    SimulationParams simulation{};
    AdversaryParams adversary{};
    std::chrono::seconds watchdog{60};
};

/// Runs a test body under the runtime of the wyrd.
void runTest(std::function<void()> body, TestParams params = {});

}
