#include <Processors/QueryPlan/AddingMissedStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AddingMissedTransform.h>
#include <IO/Operators.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false, /// TODO: check if true later.
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

AddingMissedStep::AddingMissedStep(
    const DataStream & input_stream_,
    Block result_header_,
    ColumnsDescription columns_,
    const Context & context_)
    : ITransformingStep(input_stream_, result_header_, getTraits())
    , columns(std::move(columns_))
    , context(context_)
{
    updateDistinctColumns(output_stream->header, output_stream->distinct_columns);
}

void AddingMissedStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<AddingMissedTransform>(header, output_stream->header, columns, context);
    });
}

}
