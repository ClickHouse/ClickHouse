#pragma once

#include <Core/Joins.h>
#include <Interpreters/JoinExpressionActions.h>

#include <QueryPipeline/SizeLimits.h>

#include <optional>

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

    /// Whether the ON expression contains at least two inequalities between the two inputs whose operand
    /// types the IEJoin operator can compare - the two key conditions `tryExtractIEJoinDescription` needs
    /// to plan an IEJoin (`tryGetIEJoinKeyCondition` in `JoinStepLogical.cpp` accepts exactly such a
    /// condition). Conservative: the equalities of the ON expression are claimed as hash-join keys before
    /// the inequalities are looked at unless `ie_join` heads the algorithm list, so true does not
    /// guarantee an IEJoin, but false rules it out.
    bool hasCrossSideInequalityPair() const;

    /// Whether the ON expression is a single top-level disjunction (`... OR ...`). The planning never
    /// keeps such a join as a single-clause keyed join: it either splits the disjuncts into one `TableJoin`
    /// clause each (`tryAddDisjunctiveConditions` in `JoinStepLogical.cpp`) or converts the join to CROSS.
    /// Every spilling join implementation requires the single-clause shape (`TableJoin::oneDisjunct`), so a
    /// disjunctive join can reach temporary files only through `ConstantJoin`.
    bool expressionIsTopLevelDisjunction() const;

    /// Whether a top-level conjunct of the ON expression is an equality between the two inputs - the
    /// condition `addJoinPredicatesToTableJoin` (`JoinStepLogical.cpp`) claims as a hash-join key. With
    /// none present the planning takes its no-keys paths (an IEJoin when the shape allows one, the
    /// disjunctive split, or the conversion to CROSS).
    bool hasCrossSideEqualityCondition() const;

    /// Whether a condition of the ON expression can be evaluated outside the join: pushed down into one of
    /// the inputs (`side` set) or applied above the join (`side` not set). Depends only on the kind and the
    /// strictness of the join. The filter push-down of the optimizer removes the conditions of a side this
    /// returns true for from the ON expression (`JoinStepLogical::getFilterActions`).
    bool canPushDownFromOn(std::optional<JoinTableSide> side = {}) const;

    /// Whether a top-level conjunct of the ON expression stays in the ON clause as the pre-filter condition
    /// of a join clause: a condition over a single input that the join planning attaches to the clause
    /// (`analyzer_left_filter_condition_column_name` / `analyzer_right_filter_condition_column_name` in
    /// `JoinStepLogical.cpp`). `FullSortingMergeJoin::isSupported` rejects a clause carrying one. Only a
    /// condition that cannot be pushed out of the ON expression counts: a pushable one may be gone from the
    /// expression by the time the join is built.
    bool hasSingleSidePreFilterCondition() const;

    /// Whether the join planning turns the ON expression into a mixed join expression: a condition over
    /// both inputs that is claimed neither as a hash-join key nor as a pre-filter condition of the clause,
    /// and that cannot be applied as a filter over the join result either (`build_mixed_join_expression` in
    /// `JoinStepLogical.cpp`). Only the hash family evaluates such a condition, so `MergeJoin::isSupported`
    /// and `FullSortingMergeJoin::isSupported` decline a join carrying one rather than silently dropping it.
    bool buildsMixedJoinExpression() const;

    String dump() const;
};

/// Whether the IEJoin operator can compare the operand types of one of its two key conditions, i.e. the
/// type-compatibility test of `tryGetIEJoinKeyCondition` (`JoinStepLogical.cpp`). The operator matches by
/// `compareAt`: comparison of `Tuple` decomposes elementwise (IEEE NaN, NULL propagation), and comparison
/// of `Dynamic` and `Variant` unwraps the underlying values (NULL values and mismatched alternatives yield
/// NULL or throw), so such operands are declined; the other types additionally have to share a common type
/// to be casted to.
bool ieJoinCanCompareOperandTypes(const DataTypePtr & lhs_type, const DataTypePtr & rhs_type);


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
    bool spill_codec_authorized = false;
    /// Whether this query can create temporary on-disk storage. Hash joins silently remain
    /// in memory when it is unavailable, so their external-join threshold cannot reach
    /// `temporary_files_codec` in that case.
    bool temporary_storage_available = true;
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
    bool enable_hash_join_row_store;
    Float64 min_rows_ratio_for_hash_join_row_store;

    bool enable_join_fixed_hash_table_conversion;
    bool join_runtime_filter_from_fixed_hash_table;

    /// Which statistics the join must collect for EXPLAIN ANALYZE
    JoinAnalyzeMode join_analyze_mode = JoinAnalyzeMode::None;

    explicit JoinSettings(
        const Settings & query_settings,
        JoinAnalyzeMode join_analyze_mode_ = JoinAnalyzeMode::None,
        bool temporary_storage_available_ = true);
    explicit JoinSettings(const QueryPlanSerializationSettings & settings);

    void updatePlanSettings(QueryPlanSerializationSettings & settings, const JoinOperator & join_operator, UInt64 version) const;

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
    /// (`JoinOperator::canBecomeConstantJoin`). The `join_algorithm` list is walked in the planner's
    /// first-buildable-wins order, so a spill-capable algorithm listed after one that always builds an
    /// in-memory join does not count either. Conservative: where the answer cannot be decided exactly it
    /// errs towards true, since under-reporting would make a shard reject the codec at its first spill.
    /// The external-join threshold can only reach a spilling hash join when temporary storage is available;
    /// otherwise the hash-family algorithms silently stay in memory.
    bool canSpillToTemporaryFiles(const JoinOperator & join_operator) const;

    /// Whether `chooseJoinAlgorithm`'s first-buildable-wins walk over the `join_algorithm` list always
    /// stops at this entry (its `tryCreateJoin` branch ends in an unconditional in-memory hash join), which
    /// makes every entry after it unreachable.
    static bool joinAlgorithmAlwaysBuildsSomeJoin(JoinAlgorithm algorithm);

    /// Combines the stored raw absolute and ratio settings using local memory limits.
    /// Recomputed on every executor so distributed queries pick up per-node memory.
    UInt64 getEffectiveMaxBytesBeforeExternalJoin() const
    {
        return getMaxBytesBeforeExternalJoin(max_bytes_before_external_join, max_bytes_ratio_before_external_join);
    }

    bool operator==(const JoinSettings & other) const = default;
};


}
