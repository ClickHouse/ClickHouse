#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Functions/IFunction.h>
#include <Columns/IColumn.h>

namespace DB
{

/// Forward declarations for semantic comparison functions
UInt64 getSemanticHash(const ActionsDAG::Node * node);
bool areNodesEqual(const ActionsDAG::Node * left, const ActionsDAG::Node * right);

/** This structure holds ActionsDAG node pointer and its hash. It can be used as hash map key to avoid unnecessary hash
  * recalculations.
  *
  * Similar to QueryTreeNodeWithHash but for ActionsDAG nodes.
  * Uses semantic hash that's consistent across different ActionsDAG instances.
  */
struct ActionsDAGNodeWithHash
{
    explicit ActionsDAGNodeWithHash(const ActionsDAG::Node * node_)
        : node(node_)
        , hash(node_ ? getSemanticHash(node_) : 0)
    {}

    const ActionsDAG::Node * node = nullptr;
    UInt64 hash = 0;
};

inline bool operator==(const ActionsDAGNodeWithHash & lhs, const ActionsDAGNodeWithHash & rhs)
{
    if (lhs.hash != rhs.hash)
        return false;

    if (lhs.node == rhs.node)
        return true;

    // Use semantic equality to handle nodes from different DAG instances
    return areNodesEqual(lhs.node, rhs.node);
}

inline bool operator!=(const ActionsDAGNodeWithHash & lhs, const ActionsDAGNodeWithHash & rhs)
{
    return !(lhs == rhs);
}

inline bool operator<(const ActionsDAGNodeWithHash & lhs, const ActionsDAGNodeWithHash & rhs)
{
    // Used by std::set in CNF - order by hash for consistent ordering
    return lhs.hash < rhs.hash;
}

}

template <>
struct std::hash<DB::ActionsDAGNodeWithHash>
{
    size_t operator()(const DB::ActionsDAGNodeWithHash & node_with_hash) const
    {
        return node_with_hash.hash;
    }
};
