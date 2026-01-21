#pragma once

#include <Core/SettingsEnums.h>
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
        PreparedSetsCachePtr prepared_sets_cache_,
        bool is_parallel_replicas_initiator_with_projection_support_);

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
    bool merge_filter_into_join_condition;
    bool use_join_disjunctions_push_down;
    bool convert_any_join_to_semi_or_anti_join;

    /// If we can swap probe/build tables in join
    /// true/false - always/never swap
    /// nullopt - swap if it's beneficial
    std::optional<bool> join_swap_table;
    /// Maximum number of tables in query graph to reorder
    UInt64 query_plan_optimize_join_order_limit;

    /// --- Second-pass optimizations
    bool optimize_prewhere;
    bool read_in_order;
    bool distinct_in_order;
    bool optimize_sorting_by_input_stream_properties;
    bool aggregation_in_order;
    bool optimize_projection;
    bool use_query_condition_cache;
    bool query_condition_cache_store_conditions_as_plaintext;

    /// --- Third-pass optimizations (Processors/QueryPlan/QueryPlan.cpp)
    bool build_sets = true; /// this one doesn't have a corresponding setting
    bool query_plan_join_shard_by_pk_ranges;

    bool make_distributed_plan = false;
    bool distributed_plan_singe_stage = false;  /// For debugging purposes: force distributed plan to be single-stage
    UInt64 distributed_plan_default_shuffle_join_bucket_count = 8;
    UInt64 distributed_plan_default_reader_bucket_count = 8; /// Default bucket count for read steps in distributed query plan
    bool distributed_plan_optimize_exchanges = true; /// Removes unnecessary exchanges in distributed query plan
    String distributed_plan_force_exchange_kind; /// Force exchange kind for all exchanges in distributed query plan
    UInt64 distributed_plan_max_rows_to_broadcast = 20000; /// Max number of rows to broadcast in distributed query plan
    bool distributed_plan_force_shuffle_aggregation = false; /// Force Shuffle strategy instead of PartialAggregation + Merge for distributed aggregation
    bool distributed_aggregation_memory_efficient = true; /// Is the memory-saving mode of distributed aggregation enabled

    /// ------------------------------------------------------

    /// Other settings related to plan-level optimizations

    size_t max_step_description_length = 0;

    bool optimize_use_implicit_projections;
    bool force_use_projection;
    String force_projection_name;

    /// When optimizing projections for parallel replicas reading, the initiator and the remote replicas require different handling.
    /// This parameter is used to distinguish between the initiator and the remote replicas.
    bool is_parallel_replicas_initiator_with_projection_support = false;

    /// If lazy materialization optimisation is enabled
    bool optimize_lazy_materialization = false;
    size_t max_limit_for_lazy_materialization = 0;

    /// Vector-search-related settings
    size_t max_limit_for_vector_search_queries;
    bool vector_search_with_rescoring;
    VectorSearchFilterStrategy vector_search_filter_strategy;

    /// If full text search using index in payload is enabled.
    bool direct_read_from_text_index;

    /// Setting needed for Sets (JOIN -> IN optimization)

    SizeLimits network_transfer_limits;
    size_t use_index_for_in_with_subqueries_max_values;
    PreparedSetsCachePtr prepared_sets_cache;

    /// This is needed for conversion JoinLogical -> Join

    UInt64 max_entries_for_hash_table_stats;
    UInt64 max_size_to_preallocate_for_joins;
    bool collect_hash_table_stats_during_joins;
    String initial_query_id;
    std::chrono::milliseconds lock_acquire_timeout;
    ExpressionActionsSettings actions_settings;

    /// JOIN runtime filter settings
    bool enable_join_runtime_filters = false; /// Filter left side by set of JOIN keys collected from the right side at runtime
    UInt64 join_runtime_filter_exact_values_limit = 0;
    UInt64 join_runtime_bloom_filter_bytes = 0;
    UInt64 join_runtime_bloom_filter_hash_functions = 0;

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
