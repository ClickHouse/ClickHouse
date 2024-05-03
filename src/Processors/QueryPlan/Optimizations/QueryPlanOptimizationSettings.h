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

    /// if distinct in order optimization is enabled
    bool distinct_in_order = false;

    /// If read-in-order optimisation is enabled
    bool read_in_order = true;

    /// If aggregation-in-order optimisation is enabled
    bool aggregation_in_order = false;

    /// If removing redundant sorting is enabled, for example, ORDER BY clauses in subqueries
    bool remove_redundant_sorting = true;

    bool aggregate_partitions_independently = false;

    /// If removing redundant distinct steps is enabled
    bool remove_redundant_distinct = true;

    /// If reading from projection can be applied
    bool optimize_projection = false;
    bool force_use_projection = false;
    bool optimize_use_implicit_projections = false;

    static QueryPlanOptimizationSettings fromSettings(const Settings & from);
    static QueryPlanOptimizationSettings fromContext(ContextPtr from);
};

}
