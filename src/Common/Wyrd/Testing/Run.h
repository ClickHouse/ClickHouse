#pragma once

#include <Common/Wyrd/Strategies/Adversary/AdversaryParams.h>
#include <Common/Wyrd/Strategies/Simulation/SimulationParams.h>

#include <functional>
#include <optional>

namespace DB::Wyrd
{

struct TestParams
{
    size_t runs = 1000;
    std::optional<UInt64> seed = std::nullopt;
    SimulationMode::SimulationParams simulation{};
    AdversaryMode::AdversaryParams adversary{};
};

void runAdversaryTest(std::function<void()> body, TestParams params = {});
void runSimulationTest(std::function<void()> body, TestParams params = {});

}
