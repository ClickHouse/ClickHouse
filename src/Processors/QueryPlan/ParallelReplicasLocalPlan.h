#pragma once

#include <Core/QueryProcessingStage.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

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

}
