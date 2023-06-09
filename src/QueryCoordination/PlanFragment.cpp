#include <QueryCoordination/PlanFragment.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/Context.h>
#include <memory>


namespace DB
{

QueryPipeline PlanFragment::buildQueryPipeline(const std::vector<DataSink::Channel> & channels)
{
    auto builder = query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));

    QueryPipeline pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));

    if (auto dest_fragment = getDestFragment())
    {
        String query_id;
        if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        {
            query_id = context->getCurrentQueryId();
        }
        else if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        {
            query_id = context->getInitialQueryId();
        }
        auto sink = std::make_shared<DataSink>(
            pipeline.getHeader(), channels, output_partition, query_id, dest_fragment->getFragmentId(), dest_node->plan_id);

        pipeline.complete(sink);
    }

    std::shared_ptr<const EnabledQuota> quota = context->getQuota();

    pipeline.setQuota(quota);

    return pipeline;
}

}
