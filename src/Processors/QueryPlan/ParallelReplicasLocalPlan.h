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

std::pair<QueryPlanPtr, bool> createLocalPlanForParallelReplicas(
    const QueryTreeNodePtr & query_tree,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    ParallelReplicasReadingCoordinatorPtr coordinator,
    QueryPlanStepPtr read_from_merge_tree,
    size_t replica_number);

std::vector<QueryPlan::Node *> findReadingSteps(QueryPlan::Node * root, bool allow_view_over_mergetree);

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
