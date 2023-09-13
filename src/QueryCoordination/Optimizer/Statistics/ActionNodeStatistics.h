#pragma once

#include <Interpreters/ActionsDAG.h>
#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>

namespace DB
{

using InputNodeStatsMap = std::unordered_map<const ActionsDAG::Node *, ColumnStatisticsPtr>;

/** Represent input node statistics after a predicate node applied.
 *  Note that ActionNodeStatistics only concerns about the nodes
 *  which are in the node tree.
 *
 *  The following node tree only contains stats for a, b
 *      (and)
 *      /    \
 *     >      =
 *   /   \   /  \
 *  a     1  b   2
 *
 *  Used when calculating predicate statistics.
 */
struct ActionNodeStatistics
{
    Float64 selectivity;
    InputNodeStatsMap node_stats;
};

}
