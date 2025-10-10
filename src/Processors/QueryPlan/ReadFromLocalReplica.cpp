#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/QueryPlan/FilterStep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReadFromLocalParallelReplicaStep::ReadFromLocalParallelReplicaStep(QueryPlanPtr query_plan_)
    : SourceStepWithFilterBase(query_plan_->getCurrentHeader())
    , query_plan(std::move(query_plan_))
{
}

void ReadFromLocalParallelReplicaStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} shouldn't be called", __PRETTY_FUNCTION__);
}

void ReadFromLocalParallelReplicaStep::addFilter(ActionsDAG filter_dag, std::string column_name)
{
    auto filter
        = std::make_unique<FilterStep>(query_plan->getRootNode()->step->getOutputHeader(), std::move(filter_dag), column_name, false);
    query_plan->addStep(std::move(filter));
}

QueryPlanPtr ReadFromLocalParallelReplicaStep::extractQueryPlan()
{
    return std::move(query_plan);
}
}
