#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/QueryPlan/FilterStep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReadFromLocalParallelReplicaStep::ReadFromLocalParallelReplicaStep(
    QueryPlanPtr query_plan_, ContextPtr subquery_context_, LeftOutConjuncts left_out_conjuncts_)
    : ISourceStep(query_plan_->getCurrentHeader())
    , query_plan(std::move(query_plan_))
    , context(std::move(subquery_context_))
    , left_out_conjuncts(std::move(left_out_conjuncts_))
{
}

void ReadFromLocalParallelReplicaStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} shouldn't be called", __PRETTY_FUNCTION__);
}

QueryPlanPtr ReadFromLocalParallelReplicaStep::extractQueryPlan()
{
    chassert(query_plan);

    auto qp = std::move(query_plan);
    query_plan.reset();
    return qp;
}

void ReadFromLocalParallelReplicaStep::addFilter(FilterDAGInfo filter)
{
    output_header = std::make_shared<const Block>(
        FilterTransform::transformHeader(*output_header, &filter.actions, filter.column_name, filter.do_remove_column));

    auto filter_step = std::make_unique<FilterStep>(
        query_plan->getCurrentHeader(), std::move(filter.actions), std::move(filter.column_name), filter.do_remove_column);
    query_plan->addStep(std::move(filter_step));
}

ActionsDAG::NodeRawConstPtrs ReadFromLocalParallelReplicaStep::conjunctsTheReplicasMiss(const ActionsDAG::Node & condition) const
{
    if (left_out_conjuncts)
        return left_out_conjuncts(condition);
    return ActionsDAG::extractConjunctionAtoms(&condition);
}

}
