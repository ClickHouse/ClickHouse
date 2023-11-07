#include <QueryCoordination/Optimizer/GroupStep.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

GroupStep::GroupStep(DataStream output_stream_, Group & group_) : group(group_)
{
    output_stream = std::move(output_stream_);
}

String GroupStep::getName() const
{
    return "Group (" + std::to_string(group.getId()) + ")";
}

QueryPipelineBuilderPtr GroupStep::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "'updatePipeline' is not implemented for group step.");
}

Group & GroupStep::getGroup()
{
    return group;
}

}
