#pragma once

#include <Interpreters/ActionsDAG.h>
#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>

namespace DB
{

using InputNodeStatsMap = std::unordered_map<const ActionsDAG::Node *, ColumnStatisticsPtr>;

/** Represent input node statistics after a action node applied.
 *  Note that ActionNodeStatistics only concerns about the nodes
 *  which are in the node tree.
 *
 *  Let's assume a node tree with input nodes: a, b, c
 *      (and)
 *      /    \
 *     >      =
 *   /   \   /  \
 *  a     1  b   2
 *  The node tree only contains stats for a, b.
 *
 *  Used as intermediate result when calculating action node statistics.
 */
struct ActionNodeStatistics
{
    /// Selectivity of the node
    Float64 selectivity;
    /// For constant node
    std::optional<Float64> value;
    /// Statistics after the action node
    InputNodeStatsMap input_node_stats;

    ColumnStatisticsPtr get(const ActionsDAG::Node * node)
    {
        if (input_node_stats.contains(node))
            return input_node_stats.at(node);
        return {};
    }

    /// get the unique ColumnStatisticsPtr
    ColumnStatisticsPtr get()
    {
        if (input_node_stats.size() == 1)
            return input_node_stats.begin()->second;
        return {};
    }

    void set(const ActionsDAG::Node * node, ColumnStatisticsPtr stats)
    {
        input_node_stats.insert({node, stats});
    }

};

}
