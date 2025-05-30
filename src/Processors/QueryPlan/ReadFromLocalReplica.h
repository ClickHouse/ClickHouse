#pragma once

#include <Core/QueryProcessingStage.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

class ParallelReplicasReadingCoordinator;
using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

class ReadFromLocalParallelReplicaStep : public ISourceStep
{
public:
    ReadFromLocalParallelReplicaStep(
        const ASTPtr & query_ast_,
        Block header_,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage_,
        ParallelReplicasReadingCoordinatorPtr coordinator_,
        QueryPlanStepPtr read_from_merge_tree_,
        size_t replica_number_);

    String getName() const override { return "ReadFromLocalReplica"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    std::pair<QueryPlanPtr, bool> createQueryPlan();

private:
    ASTPtr query_ast;
    ContextPtr context;
    QueryProcessingStage::Enum processed_stage;
    ParallelReplicasReadingCoordinatorPtr coordinator;
    QueryPlanStepPtr read_from_merge_tree;
    size_t replica_number;
};

}
