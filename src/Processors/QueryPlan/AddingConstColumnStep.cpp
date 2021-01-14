#include <Processors/QueryPlan/AddingConstColumnStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AddingConstColumnTransform.h>
#include <IO/Operators.h>

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

AddingConstColumnStep::AddingConstColumnStep(const DataStream & input_stream_, ColumnWithTypeAndName column_)
    : ITransformingStep(input_stream_,
                        AddingConstColumnTransform::transformHeader(input_stream_.header, column_),
                        getTraits())
    , column(std::move(column_))
{
}

void AddingConstColumnStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<AddingConstColumnTransform>(header, column);
    });
}

}
