#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/WindowTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits(bool preserves_sorting)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = preserves_sorting,
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
        column_with_type.type = f.aggregate_function->getResultType();
        column_with_type.column = column_with_type.type->createColumn();

        result.insert(column_with_type);
    }

    return result;
}

WindowStep::WindowStep(
    const Header & input_header_,
    const WindowDescription & window_description_,
    const std::vector<WindowFunctionDescription> & window_functions_,
    bool streams_fan_out_)
    : ITransformingStep(input_header_, addWindowFunctionResultColumns(input_header_, window_functions_), getTraits(!streams_fan_out_))
    , window_description(window_description_)
    , window_functions(window_functions_)
    , streams_fan_out(streams_fan_out_)
{
    // We don't remove any columns, only add, so probably we don't have to update
    // the output DataStream::distinct_columns.

    window_description.checkValid();

}

void WindowStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto num_threads = pipeline.getNumThreads();

    // This resize is needed for cases such as `over ()` when we don't have a
    // sort node, and the input might have multiple streams. The sort node would
    // have resized it.
    if (window_description.full_sort_description.empty())
        pipeline.resize(1);

    pipeline.addSimpleTransform(
        [&](const Block & /*header*/)
        {
            return std::make_shared<WindowTransform>(
                input_headers.front(), *output_header, window_description, window_functions);
        });

    if (streams_fan_out)
    {
        pipeline.resize(num_threads);
    }

    assertBlocksHaveEqualStructure(pipeline.getHeader(), *output_header,
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

void WindowStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (!window_description.partition_by.empty())
    {
        auto partion_columns_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & descr : window_description.partition_by)
            partion_columns_array->add(descr.column_name);

        map.add("Partition By", std::move(partion_columns_array));
    }

    if (!window_description.order_by.empty())
        map.add("Sort Description", explainSortDescription(window_description.order_by));

    auto functions_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & func : window_functions)
        functions_array->add(func.column_name);

    map.add("Functions", std::move(functions_array));
}

void WindowStep::updateOutputHeader()
{
    output_header = addWindowFunctionResultColumns(input_headers.front(), window_functions);

    window_description.checkValid();
}

const WindowDescription & WindowStep::getWindowDescription() const
{
    return window_description;
}

}
