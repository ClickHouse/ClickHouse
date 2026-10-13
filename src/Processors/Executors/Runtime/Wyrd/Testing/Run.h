#pragma once

#include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/AdversaryParams.h>
#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/SimulationParams.h>

#include <functional>
#include <optional>

namespace DB::Runtime::Wyrd
{

struct TestParams
{
    size_t runs = 1000;
    std::optional<UInt64> seed = std::nullopt;
    SimulationParams simulation{};
    AdversaryParams adversary{};
};

/// Runs a test body under the runtime of the wyrd.
void runTest(std::function<void()> body, TestParams params = {});

}
