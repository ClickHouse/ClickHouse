#include <Processors/QueryPlan/UpdateKeeperCursorsStep.h>
#include <Processors/Transforms/UpdateKeeperCursorsTransform.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

UpdateKeeperCursorsStep::UpdateKeeperCursorsStep(const DataStream & input_stream_, zkutil::ZooKeeperPtr zk_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits()), zk(std::move(zk_))
{
}

void UpdateKeeperCursorsStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);
    pipeline.addSimpleTransform([&](const Block & header)
                                { return std::make_shared<UpdateKeeperCursorsTransform>(header, std::move(zk)); });
}

void UpdateKeeperCursorsStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
}

}
