#include <Processors/QueryPlan/ExtractColumnsStep.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

ExtractColumnsStep::ExtractColumnsStep(const Header & input_header_, const NamesAndTypesList & requested_columns_)
    : ITransformingStep(input_header_, ExtractColumnsTransform::transformHeader(input_header_, requested_columns_), getTraits())
    , requested_columns(requested_columns_)
{
}

void ExtractColumnsStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExtractColumnsTransform>(header, requested_columns);
    });
}

void ExtractColumnsStep::updateOutputHeader()
{
    output_header = ExtractColumnsTransform::transformHeader(input_headers.front(), requested_columns);
}

}
