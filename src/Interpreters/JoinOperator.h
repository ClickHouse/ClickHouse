#pragma once

#include <Core/Joins.h>
#include <Interpreters/JoinExpressionActions.h>

#include <QueryPipeline/SizeLimits.h>

namespace DB
{

struct Settings;

struct JoinOperator
{
    /// The type of join (e.g., INNER, LEFT, RIGHT, FULL)
    JoinKind kind;

    /// The strictness of the join (e.g., ALL, ANY, SEMI, ANTI)
    JoinStrictness strictness;

    /// The locality of the join (e.g., LOCAL, GLOBAL)
    JoinLocality locality;

    /// An expression in ON/USING clause of a JOIN statement
    std::vector<JoinActionRef> expression = {};
    /// Additional filter after join (e.g. from WHERE clause)
    /// Difference is for OUTER JOINs, where expression used to match row or return NULL
    /// but residual filter is used to filter rows after join.
    /// For INNER JOINs, residual filter is the same as expression
    std::vector<JoinActionRef> residual_filter = {};

    explicit JoinOperator(
        JoinKind kind_,
        JoinStrictness strictness_ = JoinStrictness::All,
        JoinLocality locality_ = JoinLocality::Local,
        std::vector<JoinActionRef> expression_ = {})
        : kind(kind_)
        , strictness(strictness_)
        , locality(locality_)
        , expression(std::move(expression_))
    {}

    void serialize(WriteBuffer & out, const ActionsDAG * actions_dag_) const;
    static JoinOperator deserialize(ReadBuffer & in, JoinExpressionActions & expression_actions);
};


String toString(const JoinActionRef & node);

struct QueryPlanSerializationSettings;

/// Subset of query settings that are relevant to join and used to configure join algorithms.
struct JoinSettings
{
    std::vector<JoinAlgorithm> join_algorithms;

    UInt64 max_block_size;

    UInt64 max_rows_in_join;
    UInt64 max_bytes_in_join;
    UInt64 default_max_bytes_in_join;

    UInt64 max_joined_block_size_rows;
    UInt64 min_joined_block_size_bytes;

    OverflowMode join_overflow_mode;
    bool join_any_take_last_row;

    /* CROSS JOIN settings */
    UInt64 cross_join_min_rows_to_compress;
    UInt64 cross_join_min_bytes_to_compress;

    /* Partial merge join settings */
    UInt64 partial_merge_join_left_table_buffer_bytes;
    UInt64 partial_merge_join_rows_in_right_blocks;
    UInt64 join_on_disk_max_files_to_merge;

    /* Grace hash join settings */
    UInt64 grace_hash_join_initial_buckets;
    UInt64 grace_hash_join_max_buckets;

    /* Full sorting merge join settings */
    UInt64 max_rows_in_set_to_optimize_join;
    String temporary_files_codec;

    /* Hash/Parallel hash join settings */
    bool collect_hash_table_stats_during_joins;
    UInt64 max_size_to_preallocate_for_joins;
    UInt64 parallel_hash_join_threshold;
    UInt64 join_output_by_rowlist_perkey_rows_threshold;
    bool allow_experimental_join_right_table_sorting;
    UInt64 join_to_sort_minimum_perkey_rows;
    UInt64 join_to_sort_maximum_table_rows;

    explicit JoinSettings(const Settings & query_settings);
    explicit JoinSettings(const QueryPlanSerializationSettings & settings);

    void updatePlanSettings(QueryPlanSerializationSettings & settings) const;
};


}
