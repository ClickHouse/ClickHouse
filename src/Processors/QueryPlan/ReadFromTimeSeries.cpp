#include <Processors/QueryPlan/ReadFromTimeSeries.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReadFromTimeSeriesStep::ReadFromTimeSeriesStep(QueryPlanPtr query_plan_, ContextPtr read_context_)
    : ISourceStep(query_plan_->getCurrentHeader())
    , query_plan(std::move(query_plan_))
    , read_context(std::move(read_context_))
{
}

void ReadFromTimeSeriesStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} shouldn't be called: the step must be replaced with its plan during optimization", __PRETTY_FUNCTION__);
}

QueryPlanPtr ReadFromTimeSeriesStep::extractQueryPlan()
{
    chassert(query_plan);
    auto qp = std::move(query_plan);
    query_plan.reset();
    return qp;
}

}
