#include <Processors/QueryPlan/ReverseRowsStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/ReverseTransform.h>

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
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

ReverseRowsStep::ReverseRowsStep(const DataStream & input_stream_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
{
}

void ReverseRowsStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ReverseTransform>(header);
    });
}

}
