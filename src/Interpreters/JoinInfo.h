#pragma once

#include <Core/Joins.h>
#include <vector>
#include <string>
#include <ranges>
#include <optional>
#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/ActionsDAG.h>
#include <Core/Settings.h>

#include <QueryPipeline/SizeLimits.h>

namespace DB
{

enum class PredicateOperator : UInt8
{
    Equals,
    NullSafeEquals,
    Less,
    LessOrEquals,
    Greater,
    GreaterOrEquals,
};

inline std::optional<PredicateOperator> getJoinPredicateOperator(const String & func_name)
{
    if (func_name == "equals")
        return PredicateOperator::Equals;
    if (func_name == "isNotDistinctFrom")
        return PredicateOperator::NullSafeEquals;
    if (func_name == "less")
        return PredicateOperator::Less;
    if (func_name == "greater")
        return PredicateOperator::Greater;
    if (func_name == "lessOrEquals")
        return PredicateOperator::LessOrEquals;
    if (func_name == "greaterOrEquals")
        return PredicateOperator::GreaterOrEquals;
    return {};
}

inline PredicateOperator reversePredicateOperator(PredicateOperator op)
{
    switch (op)
    {
        case PredicateOperator::Equals: return PredicateOperator::Equals;
        case PredicateOperator::NullSafeEquals: return PredicateOperator::NullSafeEquals;
        case PredicateOperator::Less: return PredicateOperator::Greater;
        case PredicateOperator::Greater: return PredicateOperator::Less;
        case PredicateOperator::LessOrEquals: return PredicateOperator::GreaterOrEquals;
        case PredicateOperator::GreaterOrEquals: return PredicateOperator::LessOrEquals;
    }
}

using ActionsDAGPtr = std::unique_ptr<ActionsDAG>;

struct JoinExpressionActions
{
    JoinExpressionActions(const ColumnsWithTypeAndName & left_columns, const ColumnsWithTypeAndName & right_columns, const ColumnsWithTypeAndName & joined_columns)
        : left_pre_join_actions(std::make_unique<ActionsDAG>(left_columns))
        , right_pre_join_actions(std::make_unique<ActionsDAG>(right_columns))
        , post_join_actions(std::make_unique<ActionsDAG>(joined_columns))
    {
    }

    ActionsDAGPtr left_pre_join_actions;
    ActionsDAGPtr right_pre_join_actions;
    ActionsDAGPtr post_join_actions;
};


class JoinActionRef
{
public:
    explicit JoinActionRef(std::nullptr_t)
        : actions_dag(nullptr)
    {}

    explicit JoinActionRef(const ActionsDAG::Node * node_, const ActionsDAG * actions_dag_);

    const ActionsDAG::Node * getNode() const;

    ColumnWithTypeAndName getColumn() const;
    const String & getColumnName() const;
    DataTypePtr getType() const;

    operator bool() const { return actions_dag != nullptr; } /// NOLINT

private:
    const ActionsDAG * actions_dag = nullptr;
    String column_name;
};

/// JoinPredicate represents a single join qualifier
/// that that apply to the combination of two tables.
struct JoinPredicate
{
    JoinActionRef left_node;
    JoinActionRef right_node;
    PredicateOperator op;
};

/// JoinCondition determines if rows from two tables can be joined
struct JoinCondition
{
    /// Join predicates that must be satisfied to join rows
    std::vector<JoinPredicate> predicates;

    /// Pre-Join filters applied to the left and right tables independently
    std::vector<JoinActionRef> left_filter_conditions;
    std::vector<JoinActionRef> right_filter_conditions;

    /// Residual conditions depend on data from both tables and must be evaluated after the join has been performed.
    /// Unlike the join predicates, these conditions can be arbitrary expressions.
    std::vector<JoinActionRef> residual_conditions;
};

struct JoinExpression
{
    /// A join condition that must be satisfied to join rows
    JoinCondition condition;

    /// Alternative join conditions that can also satisfy the join.
    /// The complete join expression is: `condition OR (alt1 OR alt2 OR ... OR altN)`
    /// where alt1...altN are stored here.
    /// If any condition matches, the rows will be joined.
    std::vector<JoinCondition> disjunctive_conditions;

    /// Indicates if the join expression is defined with the USING clause
    bool is_using = false;
};

struct JoinInfo
{
    /// An expression in ON/USING clause of a JOIN statement
    JoinExpression expression;

    /// The type of join (e.g., INNER, LEFT, RIGHT, FULL)
    JoinKind kind;

    /// The strictness of the join (e.g., ALL, ANY, SEMI, ANTI)
    JoinStrictness strictness;

    /// The locality of the join (e.g., LOCAL, GLOBAL)
    JoinLocality locality;
};


std::string_view toString(PredicateOperator op);
String toString(const JoinActionRef & node);
String toString(const JoinPredicate & predicate);
String toString(const JoinCondition & condition);

#define APPLY_FOR_JOIN_SETTINGS(M) \
    M(JoinAlgorithm, join_algorithm) \
    M(UInt64, max_block_size) \
    \
    M(Bool, join_use_nulls) \
    \
    M(UInt64, max_rows_in_join) \
    M(UInt64, max_bytes_in_join) \
    \
    M(OverflowMode, join_overflow_mode) \
    M(Bool, join_any_take_last_row) \
    \
    /* CROSS JOIN settings */ \
    M(UInt64, cross_join_min_rows_to_compress) \
    M(UInt64, cross_join_min_bytes_to_compress) \
    \
    /* Partial merge join settings */ \
    M(UInt64, partial_merge_join_left_table_buffer_bytes) \
    M(UInt64, partial_merge_join_rows_in_right_blocks) \
    M(UInt64, join_on_disk_max_files_to_merge) \
    \
    /* Grace hash join settings */ \
    M(UInt64, grace_hash_join_initial_buckets) \
    M(UInt64, grace_hash_join_max_buckets) \
    \
    /* Full sorting merge join settings */ \
    M(UInt64, max_rows_in_set_to_optimize_join) \
    \
    /* Hash/Parallel hash join settings */ \
    M(Bool, collect_hash_table_stats_during_joins) \
    M(UInt64, max_size_to_preallocate_for_joins) \
    \
    M(UInt64, max_joined_block_size_rows) \
    M(String, temporary_files_codec) \
    M(UInt64, join_output_by_rowlist_perkey_rows_threshold) \
    M(UInt64, join_to_sort_minimum_perkey_rows) \
    M(UInt64, join_to_sort_maximum_table_rows) \
    M(Bool, allow_experimental_join_right_table_sorting) \
    M(UInt64, min_joined_block_size_bytes) \
    M(MaxThreads, max_threads) \
    \
    M(UInt64, default_max_bytes_in_join) \


/// Subset of query settings that are relevant to join and used to configure join algorithms.
struct JoinSettings
{
#define DECLARE_JOIN_SETTING_FILEDS(type, name) \
    SettingField##type::ValueType name;

    APPLY_FOR_JOIN_SETTINGS(DECLARE_JOIN_SETTING_FILEDS)
#undef DECLARE_JOIN_SETTING_FILEDS

    static JoinSettings create(const Settings & query_settings);
};


}
