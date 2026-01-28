#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/Transforms/FilterTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReadFromLocalParallelReplicaStep::ReadFromLocalParallelReplicaStep(QueryPlanPtr query_plan_)
    : ISourceStep(query_plan_->getCurrentHeader())
    , query_plan(std::move(query_plan_))
{
}

void ReadFromLocalParallelReplicaStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} shouldn't be called", __PRETTY_FUNCTION__);
}

QueryPlanPtr ReadFromLocalParallelReplicaStep::optimizeAndExtractQueryPlan(const QueryPlanOptimizationSettings & optimization_settings)
{
    chassert(query_plan);

    auto qp = std::move(query_plan);
    query_plan.reset();

    /// see https://github.com/ClickHouse/ClickHouse/pull/95306
    if (!optimization_settings.aggregation_in_order)
        qp->optimize(optimization_settings);
    else
    {
        auto pr_optimization_settings = optimization_settings;
        pr_optimization_settings.aggregation_in_order = false;
        qp->optimize(pr_optimization_settings);
    }
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

}
