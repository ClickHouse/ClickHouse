#pragma once

#include <Interpreters/Context_fwd.h>

#include <cstddef>

namespace DB
{

struct Settings;

struct QueryPlanOptimizationSettings
{
    /// Allows to globally disable all plan-level optimizations.
    /// Note: Even if '= true', individual optimizations may still be disabled via below settings.
    bool optimize_plan = true;

    /// If not zero, throw if too many optimizations were applied to query plan.
    /// It helps to avoid infinite optimization loop.
    size_t max_optimizations_to_apply = 0;

    /// If moving-up-of-array-join optimization is enabled.
    bool lift_up_array_join = true;

    /// If moving-limit-down optimization is enabled.
    bool push_down_limit = true;

    /// If split-filter optimization is enabled.
    bool split_filter = true;

    /// If merge-expressions optimization is enabled.
    bool merge_expressions = true;

    /// If merge-filters optimization is enabled.
    bool merge_filters = true;

    /// If filter push down optimization is enabled.
    bool filter_push_down = true;

    /// If convert OUTER JOIN to INNER JOIN optimization is enabled.
    bool convert_outer_join_to_inner_join = true;

    /// If reorder-functions-after-sorting optimization is enabled.
    bool execute_functions_after_sorting = true;

    /// If window-functions-can-use-storage-sorting optimization is enabled.
    bool reuse_storage_ordering_for_window_functions = true;

    /// If lifting-unions-up optimization is enabled.
    bool lift_up_union = true;

    /// if distinct in order optimization is enabled
    bool distinct_in_order = false;

    /// If read-in-order optimization is enabled
    bool read_in_order = true;

    /// If aggregation-in-order optimization is enabled
    bool aggregation_in_order = false;

    /// If removing redundant sorting is enabled, for example, ORDER BY clauses in subqueries
    bool remove_redundant_sorting = true;

    /// Convert Sorting to FinishSorting based on plan's sorting properties.
    bool optimize_sorting_by_input_stream_properties = true;

    /// If aggregate-partitions-independently optimization is enabled.
    bool aggregate_partitions_independently = false;

    /// If remove-redundant-distinct-steps optimization is enabled.
    bool remove_redundant_distinct = true;

    bool optimize_prewhere = true;

    /// If reading from projection can be applied
    bool optimize_projection = false;
    bool force_use_projection = false;
    String force_projection_name;
    bool optimize_use_implicit_projections = false;

    bool build_sets = true;

    bool keep_logical_steps = false;

    static QueryPlanOptimizationSettings fromSettings(const Settings & from);
    static QueryPlanOptimizationSettings fromContext(ContextPtr from);
};

}
