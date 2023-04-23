#pragma once

#include <Interpreters/Context_fwd.h>

#include <cstddef>

namespace DB
{

struct Settings;

struct QueryPlanOptimizationSettings
{
    /// If not zero, throw if too many optimizations were applied to query plan.
    /// It helps to avoid infinite optimization loop.
    size_t max_optimizations_to_apply = 0;

    /// If disabled, no optimization applied.
    bool optimize_plan = true;

    /// If filter push down optimization is enabled.
    bool filter_push_down = true;

    static QueryPlanOptimizationSettings fromSettings(const Settings & from);
    static QueryPlanOptimizationSettings fromContext(ContextPtr from);
};

}
