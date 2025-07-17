#pragma once

#include <vector>
#include <Core/Joins.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/JoinExpressionActions.h>

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
    size_t estimated_rows = 0;

    /// For join nodes
    JoinOperator join_operator;
    JoinMethod join_method = JoinMethod::None;

    /// For leaf nodes
    int relation_id = -1;

    /// Constructor for a leaf node (base relation)
    explicit DPJoinEntry(size_t id, size_t rows);

    /// Constructor for a join node
    DPJoinEntry(DPJoinEntryPtr lhs,
                DPJoinEntryPtr rhs,
                double cost_,
                size_t cardinality_,
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
    size_t estimated_rows = 0;
    std::unordered_map<String, ColumnStats> column_stats = {};

    String table_name;
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
