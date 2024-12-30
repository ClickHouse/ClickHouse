#pragma once

#include <Interpreters/Context_fwd.h>

#include <cstddef>

namespace DB
{

struct Settings;

struct QueryPlanOptimizationSettings
{
    explicit QueryPlanOptimizationSettings(const Settings & from);
    explicit QueryPlanOptimizationSettings(ContextPtr from);

    /// Allows to globally disable all plan-level optimizations.
    /// Note: Even if set to 'true', individual optimizations may still be disabled via below settings.
    bool optimize_plan;

    /// If not zero, throw if too many optimizations were applied to query plan.
    /// It helps to avoid infinite optimization loop.
    size_t max_optimizations_to_apply;

    /// ------------------------------------------------------
    /// Enable/disable plan-level optimizations

    /// --- Zero-pass optimizations (Processors/QueryPlan/QueryPlan.cpp)
    bool remove_redundant_sorting;

    /// --- First-pass optimizations
    bool lift_up_array_join;
    bool push_down_limit;
    bool split_filter;
    bool merge_expressions;
    bool merge_filters;
    bool filter_push_down;
    bool convert_outer_join_to_inner_join;
    bool execute_functions_after_sorting;
    bool reuse_storage_ordering_for_window_functions;
    bool lift_up_union;
    bool aggregate_partitions_independently;
    bool remove_redundant_distinct;
    bool try_use_vector_search;

    /// --- Second-pass optimizations
    bool optimize_prewhere;
    bool read_in_order;
    bool distinct_in_order;
    bool optimize_sorting_by_input_stream_properties;
    bool aggregation_in_order;
    bool optimize_projection;

    /// --- Third-pass optimizations (Processors/QueryPlan/QueryPlan.cpp)
    bool build_sets = true; /// this one doesn't have a corresponding setting

    /// ------------------------------------------------------

    /// Other settings related to plan-level optimizations

    bool optimize_use_implicit_projections;
    bool force_use_projection;
    String force_projection_name;

    size_t max_limit_for_ann_queries;

    /// If query condition cache is enabled, the query condition cache needs to be updated in the WHERE stage.
    bool use_query_condition_cache = false;
};

}
