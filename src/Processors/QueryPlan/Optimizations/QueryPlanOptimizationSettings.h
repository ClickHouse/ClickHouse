#pragma once

#include <cstddef>

namespace DB
{

struct Settings;

struct QueryPlanOptimizationSettings
{
    QueryPlanOptimizationSettings() = delete;
    explicit QueryPlanOptimizationSettings(const Settings & settings);

    /// If not zero, throw if too many optimizations were applied to query plan.
    /// It helps to avoid infinite optimization loop.
    size_t max_optimizations_to_apply = 0;
};

}
