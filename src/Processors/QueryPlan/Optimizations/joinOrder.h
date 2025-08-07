#pragma once

#include <vector>
#include <Core/Joins.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

namespace DB
{

struct DPJoinEntry;
using DPJoinEntryPtr = std::shared_ptr<DPJoinEntry>;

enum class JoinMethod : UInt8
{
    None,
    Hash,
    Merge,
};

struct DPJoinEntry
{
    BitSet relations;

    DPJoinEntryPtr left;
    DPJoinEntryPtr right;

    double cost = 0.0;
    std::optional<UInt64> estimated_rows = {};

    /// For join nodes
    JoinOperator join_operator;
    JoinMethod join_method = JoinMethod::None;

    /// For leaf nodes
    int relation_id = -1;

    /// Constructor for a leaf node (base relation)
    explicit DPJoinEntry(size_t id, std::optional<UInt64> rows);

    /// Constructor for a join node
    DPJoinEntry(DPJoinEntryPtr lhs,
                DPJoinEntryPtr rhs,
                double cost_,
                std::optional<UInt64> cardinality_,
                JoinOperator join_operator_,
                JoinMethod join_method_ = JoinMethod::Hash);

    bool isLeaf() const;

    String dump() const;
};

struct ColumnStats
{
    UInt64 num_distinct_values;
};

struct RelationStats
{
    std::optional<UInt64> estimated_rows = {};
    std::unordered_map<String, ColumnStats> column_stats = {};

    String table_name;
    ConditionSelectivityEstimatorPtr estimator = nullptr;

    void materialize(ActionsDAG::Node * node = nullptr)
    {
        if (estimator == nullptr)
            return;
        RelationProfile profile;
        if (node == nullptr)
        {
            profile = estimator->estimateRelationProfile();
        }
        else
        {
            profile = estimator->estimateRelationProfile(node);
        }
        estimated_rows = static_cast<UInt64>(profile.rows);
        for (const auto & [col_name, col_profile] : profile.column_profile)
        {
            column_stats.emplace(col_name, ColumnStats{static_cast<UInt64>(col_profile.num_distinct_values)});
        }
        estimator = nullptr;
    }
};

struct QueryGraph
{
    std::vector<RelationStats> relation_stats;

    std::vector<JoinActionRef> edges;

    std::vector<std::tuple<BitSet, BitSet, JoinKind>> dependencies;
    std::unordered_map<JoinActionRef, BitSet> pinned;
};


DPJoinEntryPtr optimizeJoinOrder(QueryGraph query_graph);

}
