#include <QueryCoordination/PlanFragment.h>
#include <Processors/Sinks/DataSink.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <memory>


namespace DB
{

void PlanFragment::finalize()
{
    if (sink)
        return;

    if (dest_node)
    {
        // we're streaming to an exchange node
        std::shared_ptr<DataSink> data_sink = std::make_shared<DataSink>(dest_node->plan_id);
        data_sink->setPartition(output_partition);
//        data_sink->setFragment(this);
        query_plan.addStep(data_sink);
        sink = query_plan.getRootNode();
    }
    else
    {
        // root fragment
        // TODO
    }
}


void PlanFragment::buildQueryPipeline()
{
    auto builder = query_plan.buildQueryPipeline(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));

//    pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
//
//    setQuota(res.pipeline);
}

}
