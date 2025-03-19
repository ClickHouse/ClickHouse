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

class BaseRelsSet : public std::bitset<64>
{
public:
    BaseRelsSet(UInt64 value) : std::bitset<64>(value) {}
    BaseRelsSet() : std::bitset<64>(0) {}
    BaseRelsSet(const std::bitset<64> & value) : std::bitset<64>(value) {}
};

}

template <>
struct std::hash<DB::BaseRelsSet>
{
    size_t operator()(const DB::BaseRelsSet & rels) const { return std::hash<std::bitset<64>>{}(rels); }
};

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

void serializePredicateOperator(PredicateOperator op, WriteBuffer & out);
PredicateOperator deserializePredicateOperator(ReadBuffer & in);

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

class JoinActionRef
{
public:
    explicit JoinActionRef(std::nullptr_t);
    explicit JoinActionRef(const ActionsDAG::Node * node_, BaseRelsSet src_rels_);
    explicit JoinActionRef(const ActionsDAG::Node * node_, ActionsDAG * actions_dag_);

    const ActionsDAG::Node * getNode() const;
    ActionsDAG * getActions() const;
    void setActions(ActionsDAG * actions_dag_);

    ColumnWithTypeAndName getColumn() const;
    const String & getColumnName() const;
    DataTypePtr getType() const;

    operator bool() const { return !column_name.empty(); } /// NOLINT
    bool canBeCalculated(BaseRelsSet rels) const;

    using ActionsDAGRawPtrs = std::vector<const ActionsDAG *>;

    void serialize(WriteBuffer & out, const ActionsDAGRawPtrs & dags) const;
    static JoinActionRef deserialize(ReadBuffer & in, const ActionsDAGRawPtrs & dags);

private:
    String column_name;
    BaseRelsSet src_rels;

    ActionsDAG * actions_dag = nullptr;
};

/// JoinPredicate represents a single join qualifier
/// that that apply to the combination of two tables.
struct JoinPredicate
{
    JoinActionRef left_node;
    JoinActionRef right_node;
    PredicateOperator op;

    void serialize(WriteBuffer & out, const JoinActionRef::ActionsDAGRawPtrs & dags) const;
    static JoinPredicate deserialize(ReadBuffer & in, const JoinActionRef::ActionsDAGRawPtrs & dags);
};

/// JoinCondition determines if rows from two tables can be joined
struct JoinCondition
{
    /// Join predicates that must be satisfied to join rows
    std::vector<JoinPredicate> predicates;

    /// Additional restrictions that must be satisfied
    std::vector<JoinActionRef> restrict_conditions;

    void serialize(WriteBuffer & out, const JoinActionRef::ActionsDAGRawPtrs & dags) const;
    static JoinCondition deserialize(ReadBuffer & in, const JoinActionRef::ActionsDAGRawPtrs & dags);
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

    void serialize(WriteBuffer & out, const JoinActionRef::ActionsDAGRawPtrs & dags) const;
    static JoinExpression deserialize(ReadBuffer & in, const JoinActionRef::ActionsDAGRawPtrs & dags);
};

struct JoinExpressionActions
{
    JoinExpressionActions() = default;
    JoinExpressionActions(const JoinExpressionActions &) = delete;
    JoinExpressionActions & operator=(const JoinExpressionActions &) = delete;
    JoinExpressionActions(JoinExpressionActions &&) = default;
    JoinExpressionActions & operator=(JoinExpressionActions &&) = default;

    ActionsDAGPtr & getActions(BaseRelsSet sources, const std::vector<ColumnsWithTypeAndName> & tables);

    std::unordered_map<BaseRelsSet, ActionsDAGPtr> actions;
};

struct JoinOperator
{
    /// The type of join (e.g., INNER, LEFT, RIGHT, FULL)
    JoinKind kind;

    /// The strictness of the join (e.g., ALL, ANY, SEMI, ANTI, ASOF)
    JoinStrictness strictness;

    /// The locality of the join (e.g., LOCAL, GLOBAL)
    JoinLocality locality;

    /// An expression in ON/USING clause of a JOIN statement
    JoinExpression expression = {};

    JoinExpressionActions expression_actions = {};

    JoinOperator() = default;
    JoinOperator(JoinOperator &&) = default;
    JoinOperator & operator=(JoinOperator &&) = default;
    JoinOperator(const JoinOperator &) = delete;
    JoinOperator & operator=(const JoinOperator &) = delete;

    JoinOperator(JoinKind kind_, JoinStrictness strictness_, JoinLocality locality_)
        : kind(kind_), strictness(strictness_), locality(locality_) {}

    void serialize(WriteBuffer & out, const JoinActionRef::ActionsDAGRawPtrs & dags) const;

    static JoinOperator deserialize(ReadBuffer & in, const JoinActionRef::ActionsDAGRawPtrs & dags);
};


std::string_view toString(PredicateOperator op);
String toString(const JoinActionRef & node);
String toString(const JoinPredicate & predicate);
String toString(const JoinCondition & condition);

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
    UInt64 join_output_by_rowlist_perkey_rows_threshold;
    bool allow_experimental_join_right_table_sorting;
    UInt64 join_to_sort_minimum_perkey_rows;
    UInt64 join_to_sort_maximum_table_rows;

    explicit JoinSettings(const Settings & query_settings);
    explicit JoinSettings(const QueryPlanSerializationSettings & settings);

    void updatePlanSettings(QueryPlanSerializationSettings & settings) const;
};


}
