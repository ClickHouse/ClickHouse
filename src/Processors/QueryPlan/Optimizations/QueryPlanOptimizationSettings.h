#pragma once

#include <Interpreters/Context_fwd.h>

#include <cstddef>

namespace DB
{

struct Settings;

struct QueryPlanOptimizationSettings
{
    /// Allows to globally disable all plan-level optimizations.
    /// Note: Even if set to 'true', individual optimizations may still be disabled via below settings.
    bool optimize_plan = true;

    /// If not zero, throw if too many optimizations were applied to query plan.
    /// It helps to avoid infinite optimization loop.
    size_t max_optimizations_to_apply = 0;

    /// ------------------------------------------------------
    /// Enable/disable plan-level optimizations

    /// --- Zero-pass optimizations (Processors/QueryPlan/QueryPlan.cpp)
    bool remove_redundant_sorting = true;

    /// --- First-pass optimizations
    bool lift_up_array_join = true;
    bool push_down_limit = true;
    bool split_filter = true;
    bool merge_expressions = true;
    bool merge_filters = true;
    bool filter_push_down = true;
    bool convert_outer_join_to_inner_join = true;
    bool execute_functions_after_sorting = true;
    bool reuse_storage_ordering_for_window_functions = true;
    bool lift_up_union = true;
    bool aggregate_partitions_independently = false;
    bool remove_redundant_distinct = true;
    bool try_use_vector_search = true;

    /// --- Second-pass optimizations
    bool optimize_prewhere = true;
    bool read_in_order = true;
    bool distinct_in_order = false;
    bool optimize_sorting_by_input_stream_properties = true;
    bool aggregation_in_order = false;
    bool optimize_projection = false;

    /// --- Third-pass optimizations (Processors/QueryPlan/QueryPlan.cpp)
    bool build_sets = true;

    /// ------------------------------------------------------

    /// Other settings related to plan-level optimizations
    bool optimize_use_implicit_projections = false;
    bool force_use_projection = false;
    String force_projection_name;
    size_t max_limit_for_ann_queries = 1'000'000;

    static QueryPlanOptimizationSettings fromSettings(const Settings & from);
    static QueryPlanOptimizationSettings fromContext(ContextPtr from);
};

}
