#include <Processors/QueryPlan/ParallelReplicasLocalPlan.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>

namespace DB
{

ReadFromLocalParallelReplicaStep::ReadFromLocalParallelReplicaStep(
    const ASTPtr & query_ast_,
    Block header_,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage_,
    ParallelReplicasReadingCoordinatorPtr coordinator_,
    QueryPlanStepPtr read_from_merge_tree_,
    size_t replica_number_)
    : ISourceStep(std::move(header_))
    , query_ast(query_ast_)
    , context(context_)
    , processed_stage(processed_stage_)
    , coordinator(coordinator_)
    , read_from_merge_tree(std::move(read_from_merge_tree_))
    , replica_number(replica_number_)
{
}

void ReadFromLocalParallelReplicaStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} shouldn't be called", __PRETTY_FUNCTION__);
}

std::pair<QueryPlanPtr, bool> ReadFromLocalParallelReplicaStep::createQueryPlan()
{
    return createLocalPlanForParallelReplicas(
        query_ast, getOutputHeader(), context, processed_stage, coordinator, std::move(read_from_merge_tree), replica_number);
}
}
