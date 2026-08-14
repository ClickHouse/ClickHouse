#pragma once

#include <Core/QueryProcessingStage.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>
#include <Client/ConnectionPool_fwd.h>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;
class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

std::shared_ptr<const QueryPlan> createRemotePlanForParallelReplicas(
    const QueryTreeNodePtr & query_tree,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage);

/// The context whose settings the shipped fragment runs under on the remote replicas: the fragment is sent
/// as an AST carrying the `SETTINGS` of its own root `QueryNode` / `UnionNode`, not those of the outer query.
/// Returns `fallback` when the root is neither of those.
ContextPtr getShippedFragmentContext(const QueryTreeNodePtr & query_tree, ContextPtr fallback);

std::pair<QueryPlanPtr, bool> createLocalPlanForParallelReplicas(
    const QueryTreeNodePtr & query_tree,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    ParallelReplicasReadingCoordinatorPtr coordinator,
    QueryPlanStepPtr read_from_merge_tree,
    size_t replica_number);

std::vector<QueryPlan::Node *> findReadingSteps(QueryPlan::Node * root, bool allow_view_over_mergetree, bool * right_branch_selected = nullptr);

QueryPlanPtr createLocalPlanFragmentForParallelReplicas(
    ContextPtr context,
    QueryPlanPtr plan_fragment,
    ParallelReplicasReadingCoordinatorPtr coordinator,
    size_t replica_number);

QueryPlanPtr createRemotePlanFragmentForParallelReplicas(
    ContextPtr context,
    QueryPlanPtr plan_fragment,
    ParallelReplicasReadingCoordinatorPtr coordinator,
    const ClusterPtr & cluster,
    const std::vector<ConnectionPoolPtr> & connection_pool,
    std::optional<size_t> exclude_pool_index);
}
