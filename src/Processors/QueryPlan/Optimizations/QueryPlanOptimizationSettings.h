#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <QueryPipeline/SizeLimits.h>

#include <cstddef>

namespace DB
{

struct Settings;

class PreparedSetsCache;
using PreparedSetsCachePtr = std::shared_ptr<PreparedSetsCache>;

struct QueryPlanOptimizationSettings
{
    explicit QueryPlanOptimizationSettings(
        const Settings & from,
        UInt64 max_entries_for_hash_table_stats_,
        String initial_query_id_,
        ExpressionActionsSettings actions_settings_,
        PreparedSetsCachePtr prepared_sets_cache_);

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
    bool convert_join_to_in;

    /// If we can swap probe/build tables in join
    /// true/false - always/never swap
    /// nullopt - swap if it's beneficial
    std::optional<bool> join_swap_table;

    /// --- Second-pass optimizations
    bool optimize_prewhere;
    bool read_in_order;
    bool distinct_in_order;
    bool optimize_sorting_by_input_stream_properties;
    bool aggregation_in_order;
    bool optimize_projection;
    bool use_query_condition_cache = false;

    /// --- Third-pass optimizations (Processors/QueryPlan/QueryPlan.cpp)
    bool build_sets = true; /// this one doesn't have a corresponding setting
    bool query_plan_join_shard_by_pk_ranges;

    /// ------------------------------------------------------

    /// Other settings related to plan-level optimizations

    bool optimize_use_implicit_projections;
    bool force_use_projection;
    String force_projection_name;

    /// If lazy materialization optimisation is enabled
    bool optimize_lazy_materialization = false;
    size_t max_limit_for_lazy_materialization = 0;

    size_t max_limit_for_ann_queries;

    /// Setting needed for Sets (JOIN -> IN optimization)

    SizeLimits network_transfer_limits;
    size_t use_index_for_in_with_subqueries_max_values;
    PreparedSetsCachePtr prepared_sets_cache;

    /// This is needed for conversion JoinLogical -> Join

    UInt64 max_entries_for_hash_table_stats;
    String initial_query_id;
    std::chrono::milliseconds lock_acquire_timeout;
    ExpressionActionsSettings actions_settings;

    /// Please, avoid using this
    ///
    /// We should not have the number of threads in query plan.
    /// The information about threads should be available only at the moment we build pipeline.
    /// Currently, it is used by ConcurrentHashJoin: it requiers the number of threads in ctor.
    /// It should be relativaly simple to fix, but I will do it later.
    size_t max_threads;

    bool keep_logical_steps;

    bool is_explain;
};

}
