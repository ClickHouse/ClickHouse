#include <Processors/QueryPlan/CountAffectedRowsStep.h>

#include <QueryPipeline/QueryPipelineBuilder.h>


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

CountAffectedRowsStep::CountAffectedRowsStep(
    const SharedHeader & input_header_, String command_index_column_name_, AffectedRowsCounterPtr counter_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , command_index_column_name(std::move(command_index_column_name_))
    , counter(std::move(counter_))
{
}

void CountAffectedRowsStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<CountAffectedRowsTransform>(header, command_index_column_name, counter);
    });
}

}
