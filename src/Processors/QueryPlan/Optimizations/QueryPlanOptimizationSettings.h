#pragma once

#include <cstddef>

namespace DB
{

struct Settings;
class Context;

struct QueryPlanOptimizationSettings
{
    /// If not zero, throw if too many optimizations were applied to query plan.
    /// It helps to avoid infinite optimization loop.
    size_t max_optimizations_to_apply = 0;

    static QueryPlanOptimizationSettings fromSettings(const Settings & from);
    static QueryPlanOptimizationSettings fromContext(const Context & from);
};

}
