#include <QueryCoordination/Optimizer/GroupStep.h>

namespace DB
{


GroupStep::GroupStep(DataStream output_stream_, Group & group_) : group(group_)
{
    output_stream = std::move(output_stream_);
}

String GroupStep::getName() const
{
    return "Group (" + std::to_string(group.getId()) + ")";
}

QueryPipelineBuilderPtr GroupStep::updatePipeline(QueryPipelineBuilders /*pipelines*/, const BuildQueryPipelineSettings & /*settings*/)
{
    throw;
}

Group & GroupStep::getGroup()
{
    return group;
}

}
