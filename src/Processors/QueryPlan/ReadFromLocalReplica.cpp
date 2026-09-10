#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/QueryPlan/FilterStep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReadFromLocalParallelReplicaStep::ReadFromLocalParallelReplicaStep(QueryPlanPtr query_plan_, ContextPtr subquery_context_)
    : ISourceStep(query_plan_->getCurrentHeader())
    , query_plan(std::move(query_plan_))
    , context(std::move(subquery_context_))
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

void ReadFromLocalParallelReplicaStep::restrictFixedColumnsToOwnFilters()
{
    if (!query_plan || !query_plan->isInitialized())
        return;

    const auto own = QueryPlanOptimizations::collectFixedColumnNames(*query_plan->getRootNode());
    std::vector<QueryPlan::Node *> stack{query_plan->getRootNode()};
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();
        if (auto * reading = typeid_cast<ReadFromMergeTree *>(node->step.get()))
            reading->restrictFixedColumns(own);
        for (auto * child : node->children)
            stack.push_back(child);
    }
}

void ReadFromLocalParallelReplicaStep::addFilter(FilterDAGInfo filter)
{
    output_header = std::make_shared<const Block>(
        FilterTransform::transformHeader(*output_header, &filter.actions, filter.column_name, filter.do_remove_column));

    auto filter_step = std::make_unique<FilterStep>(
        query_plan->getCurrentHeader(), std::move(filter.actions), std::move(filter.column_name), filter.do_remove_column);
    query_plan->addStep(std::move(filter_step));
}

}
