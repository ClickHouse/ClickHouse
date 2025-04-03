#pragma once

#include <Core/Joins.h>
#include <Interpreters/JoinOperator.h>

namespace DB
{

struct DPJoinEntry;
using DPJoinEntryPtr = std::shared_ptr<DPJoinEntry>;


enum class JoinMethod : UInt8
{
    Hash,
    Merge,
};

struct DPJoinEntry
{
    BaseRelsSet relations;

    DPJoinEntryPtr left;
    DPJoinEntryPtr right;

    double cost;
    size_t estimated_rows;

    /// For join nodes
    JoinOperator * join_operator;
    JoinMethod join_method;

    /// For leaf nodes
    int relation_id = -1;

    /// Constructor for a leaf node (base relation)
    explicit DPJoinEntry(size_t id, size_t rows);

    /// Constructor for a join node
    DPJoinEntry(DPJoinEntryPtr lhs,
                DPJoinEntryPtr rhs,
                double cost_,
                size_t cardinality_,
                JoinOperator * join_operator_,
                JoinMethod join_method_ = JoinMethod::Hash);

    bool isLeaf() const;
};

class JoinStepLogical;

DPJoinEntryPtr optimizeJoinOrder(JoinStepLogical & join_step);

}
