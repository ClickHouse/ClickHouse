#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <optional>
#include <vector>

namespace DB
{

class ReadFromMergeTree;

namespace QueryPlanOptimizations
{

struct PushDownPruningTarget
{
    ReadFromMergeTree * reading = nullptr;
    /// Expression DAGs between the join input and the read step, top to bottom
    std::vector<const ActionsDAG *> expression_dags;
};

/// Find the MergeTree read step a pushed-down filter would eventually prune, together with the
/// expression chain the filter is composed through (mirrors optimizePrimaryKeyConditionAndLimit).
/// Returns nothing when the subplan does not end in a plain Expression/Filter chain over
/// ReadFromMergeTree, in which case the usefulness of a pushed filter cannot be judged.
std::optional<PushDownPruningTarget> findPushDownPruningTarget(const QueryPlan::Node * node);

/// Find the MergeTree read an inferred/redundant condition on `column_name` would prune:
/// either straight below `node`, or - when `node` is a join - below the join side whose
/// output header contains the column (push-down would move the condition there).
std::optional<PushDownPruningTarget> findPruningTargetForColumn(const QueryPlan::Node * node, const std::string & column_name);

/// Whether the single-output candidate filter can participate in index pruning on the target:
/// primary key, partition key / minmax, a per-part-usable skipping index, or part virtual columns.
/// Mirrors the real index analysis the read step performs (ReadFromMergeTree::buildIndexes).
bool pushedPredicateHelpsPruning(ActionsDAG candidate_dag, const PushDownPruningTarget & target);

}

}
