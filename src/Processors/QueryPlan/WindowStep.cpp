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

static Block addWindowFunctionResultColumns(const Block & block,
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
            addWindowFunctionResultColumns(input_stream_.header,
                window_functions_),
        getTraits())
    , window_description(window_description_)
    , window_functions(window_functions_)
    , input_header(input_stream_.header)
{
    // We don't remove any columns, only add, so probably we don't have to update
    // the output DataStream::distinct_columns.
}

void WindowStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & /*header*/)
    {
        return std::make_shared<WindowTransform>(input_header,
            output_stream->header, window_description, window_functions);
    });

    assertBlocksHaveEqualStructure(pipeline.getHeader(), output_stream->header,
        "WindowStep transform for '" + window_description.window_name + "'");
}

void WindowStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Window: (";
    if (!window_description.partition_by.empty())
    {
        settings.out << "PARTITION BY ";
        for (size_t i = 0; i < window_description.partition_by.size(); ++i)
        {
            if (i > 0)
            {
                settings.out << ", ";
            }

            settings.out << window_description.partition_by[i].column_name;
        }
    }
    if (!window_description.partition_by.empty()
        && !window_description.order_by.empty())
    {
        settings.out << " ";
    }
    if (!window_description.order_by.empty())
    {
        settings.out << "ORDER BY "
            << dumpSortDescription(window_description.order_by);
    }
    settings.out << ")\n";

    for (size_t i = 0; i < window_functions.size(); ++i)
    {
        settings.out << prefix << (i == 0 ? "Functions: "
                                          : "           ");
        settings.out << window_functions[i].column_name << "\n";
    }
}

}
