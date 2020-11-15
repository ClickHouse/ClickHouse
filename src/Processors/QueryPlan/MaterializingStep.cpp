#include <Processors/QueryPlan/MaterializingStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/MaterializingTransform.h>

#include <DataStreams/materializeBlock.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

MaterializingStep::MaterializingStep(const DataStream & input_stream_)
    : ITransformingStep(input_stream_, materializeBlock(input_stream_.header), getTraits())
{
}

void MaterializingStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<MaterializingTransform>(header);
    });
}

}
