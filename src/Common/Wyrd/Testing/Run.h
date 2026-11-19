#pragma once

#include <Common/Wyrd/Namespace.h>
#include <Common/Wyrd/Strategies/Adversary/AdversaryParams.h>
#include <Common/Wyrd/Strategies/Simulation/SimulationParams.h>

#include <functional>
#include <optional>

namespace WYRD_NAMESPACE(DB::Wyrd)
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
