#include <Processors/QueryPlan/WindowStep.h>

#include <Processors/Transforms/WindowTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/QueryPipeline.h>
#include <Interpreters/ExpressionActions.h>
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
            .preserves_number_of_rows = true
        }
    };
}

static Block addWindowFunctionColumns(const Block & block,
    std::vector<WindowFunctionDescription> window_functions)
{
    auto result = block;

    for (const auto & f : window_functions)
    {
        ColumnWithTypeAndName column_with_type;
        column_with_type.name = f.column_name;
        column_with_type.type = f.aggregate_function->getReturnType();
        column_with_type.column = column_with_type.type->createColumn();

        result.insert(column_with_type);
    }

    return result;
}

WindowStep::WindowStep(const DataStream & input_stream_,
        const WindowDescription & window_description_,
        const std::vector<WindowFunctionDescription> & window_functions_)
    : ITransformingStep(
        input_stream_,
            addWindowFunctionColumns(input_stream_.header, window_functions_),
        getTraits())
    , window_description(window_description_)
    , window_functions(window_functions_)
    , input_header(input_stream_.header)
{
    /// Some columns may be removed by expression.
    updateDistinctColumns(output_stream->header, output_stream->distinct_columns);
}

void WindowStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & /*header*/)
    {
        return std::make_shared<Transform>(input_header,
            output_stream->header, window_description, window_functions);
    });

    assertBlocksHaveEqualStructure(pipeline.getHeader(), output_stream->header,
        "WindowStep transform for '" + window_description.window_name + "'");
}

void WindowStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    (void) prefix;
    /// FIXME add some printing
}

}
