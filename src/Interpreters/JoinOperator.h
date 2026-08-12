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

    /// (filter_name, build-side key column name) pairs that HashJoin should publish as
    /// shared FixedHashMap runtime filters. Set by the joinRuntimeFilter optimizer pass.
    std::vector<std::pair<String, String>> shared_runtime_filter_descriptors = {};

    explicit JoinOperator(
        JoinKind kind_ = JoinKind::Cross,
        JoinStrictness strictness_ = JoinStrictness::All,
        JoinLocality locality_ = JoinLocality::Unspecified,
        std::vector<JoinActionRef> expression_ = {})
        : kind(kind_)
        , strictness(strictness_)
        , locality(locality_)
        , expression(std::move(expression_))
    {}

    void serialize(WriteBuffer & out, const ActionsDAG * actions_dag_) const;
    static JoinOperator deserialize(ReadBuffer & in, JoinExpressionActions & expression_actions);

    /// Whether the planner can end up executing this join with `ConstantJoin` (a CROSS / comma join, or a
    /// join whose predicate degenerates to a constant). A join keyed by an equality between the two inputs
    /// always keeps its hash-join clause and can be neither a join with a constant predicate nor converted
    /// to CROSS, so for it this returns false. Conservative: true whenever `ConstantJoin` cannot be ruled out.
    bool canBecomeConstantJoin() const;

    /// Whether the ON expression contains at least two inequalities between the two inputs - the shape
    /// `tryExtractIEJoinDescription` needs to plan an IEJoin. Conservative: the type-compatibility part of
    /// that test is not repeated here, so true does not guarantee an IEJoin, but false rules it out.
    bool hasCrossSideInequalityPair() const;

    String dump() const;
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
    UInt64 max_joined_block_size_bytes;
    UInt64 min_joined_block_size_rows;
    UInt64 min_joined_block_size_bytes;
    bool joined_block_split_single_row;
    bool parallel_non_joined_rows_processing;

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

    /* Spilling hash join settings */
    UInt64 max_bytes_before_external_join = 0;
    double max_bytes_ratio_before_external_join = 0;

    /* Full sorting merge join settings */
    UInt64 max_rows_in_set_to_optimize_join;
    String temporary_files_codec;
    bool allow_experimental_codecs = false;
    UInt64 temporary_files_buffer_size;

    /* Hash/Parallel hash join settings */
    bool collect_hash_table_stats_during_joins;
    UInt64 max_size_to_preallocate_for_joins;
    UInt64 parallel_hash_join_threshold;
    UInt64 join_output_by_rowlist_perkey_rows_threshold;
    bool allow_experimental_join_right_table_sorting;
    UInt64 join_to_sort_minimum_perkey_rows;
    UInt64 join_to_sort_maximum_table_rows;
    bool allow_dynamic_type_in_join_keys;

    bool use_join_disjunctions_push_down;
    bool enable_lazy_columns_replication;
    bool enable_software_prefetch_in_join;
    bool use_hash_table_stats_for_join_reordering;

    bool enable_join_fixed_hash_table_conversion;
    bool join_runtime_filter_from_fixed_hash_table;

    explicit JoinSettings(const Settings & query_settings);
    explicit JoinSettings(const QueryPlanSerializationSettings & settings);

    void updatePlanSettings(QueryPlanSerializationSettings & settings, const JoinOperator & join_operator) const;

    /// Returns the effective threshold for converting a hash join into a grace hash join (spilling to disk),
    /// combining the absolute `max_bytes_before_external_join` and the ratio `max_bytes_ratio_before_external_join`
    /// (the smaller of the two applies). Returns 0 if neither is set, meaning no automatic spilling.
    static UInt64 getMaxBytesBeforeExternalJoin(UInt64 max_bytes_before_external_join, double max_bytes_ratio_before_external_join);

    /// Whether a join over `join_operator` built from these settings can reach temporary files, i.e.
    /// whether `temporary_files_codec` can ever be resolved. `false` for the in-memory-only
    /// configurations, which therefore need not carry the spill-codec opt-in in a serialized plan: the
    /// external-join thresholds matter only for the algorithms that convert into a `SpillingHashJoin`,
    /// `partial_merge` / `auto` spill through `MergeJoin` only for the kind/strictness pairs it supports,
    /// and the in-memory size limits (`max_rows_in_join` / `max_bytes_in_join`) trigger spilling only in
    /// `ConstantJoin`, so they count only when the join shape admits one
    /// (`JoinOperator::canBecomeConstantJoin`). Conservative: the parts of the planners' tests that need
    /// the full `TableJoin` (a single disjunct, no mixed expression) are unknowable here and count as
    /// satisfied, since under-reporting would make a shard reject the codec at its first spill.
    bool canSpillToTemporaryFiles(const JoinOperator & join_operator) const;

    /// Combines the stored raw absolute and ratio settings using local memory limits.
    /// Recomputed on every executor so distributed queries pick up per-node memory.
    UInt64 getEffectiveMaxBytesBeforeExternalJoin() const
    {
        return getMaxBytesBeforeExternalJoin(max_bytes_before_external_join, max_bytes_ratio_before_external_join);
    }

    bool operator==(const JoinSettings & other) const = default;
};


}
