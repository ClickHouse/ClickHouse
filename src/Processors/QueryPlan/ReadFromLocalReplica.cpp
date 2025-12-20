#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/QueryPlan/FilterStep.h>

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

QueryPlanPtr ReadFromLocalParallelReplicaStep::extractQueryPlan()
{
    chassert(query_plan);

    auto qp = std::move(query_plan);
    query_plan.reset();

    for (const auto & filter_info : pushed_down_filters)
    {
        auto filter_step = std::make_unique<FilterStep>(
            qp->getCurrentHeader(), filter_info.actions.clone(), filter_info.column_name, filter_info.do_remove_column);

        qp->addStep(std::move(filter_step));
    }

    return qp;
}

void ReadFromLocalParallelReplicaStep::addFilterDAGInfo(FilterDAGInfo filter)
{
    output_header = std::make_shared<const Block>(FilterTransform::transformHeader(
            *output_header,
            &filter.actions,
            filter.column_name,
            filter.do_remove_column));
    pushed_down_filters.push_back(std::move(filter));
}

}
