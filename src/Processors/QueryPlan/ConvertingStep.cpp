#include <Processors/QueryPlan/ConvertingStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/ConvertingTransform.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits
    {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
    };
}

ConvertingStep::ConvertingStep(const DataStream & input_stream_, Block result_header_)
    : ITransformingStep(input_stream_, result_header_, getTraits())
    , result_header(std::move(result_header_))
{
    updateDistinctColumns(output_stream->header, output_stream->distinct_columns);
}

void ConvertingStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ConvertingTransform>(header, result_header, ConvertingTransform::MatchColumnsMode::Name);
    });
}

}
