#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/FilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FilterStep::FilterStep(
    const DataStream & input_stream_,
    ActionsDAG actions_dag_,
    String filter_column_name_,
    bool remove_filter_column_)
    : ITransformingStep(
        input_stream_,
        FilterTransform::transformHeader(
            input_stream_.header,
            &actions_dag_,
            filter_column_name_,
            remove_filter_column_),
        getTraits())
    , actions_dag(std::move(actions_dag_))
    , filter_column_name(std::move(filter_column_name_))
    , remove_filter_column(remove_filter_column_)
{
    actions_dag.removeAliasesForFilter(filter_column_name);
}

void FilterStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto expression = std::make_shared<ExpressionActions>(std::move(actions_dag), settings.getActionsSettings());

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        return std::make_shared<FilterTransform>(header, expression, filter_column_name, remove_filter_column, on_totals);
    });

    if (!blocksHaveEqualStructure(pipeline.getHeader(), output_stream->header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                pipeline.getHeader().getColumnsWithTypeAndName(),
                output_stream->header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);
        auto convert_actions = std::make_shared<ExpressionActions>(std::move(convert_actions_dag), settings.getActionsSettings());

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, convert_actions);
        });
    }
}

void FilterStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, settings.indent_char);
    settings.out << prefix << "Filter column: " << filter_column_name;

    if (remove_filter_column)
        settings.out << " (removed)";
    settings.out << '\n';

    auto expression = std::make_shared<ExpressionActions>(actions_dag.clone());
    expression->describeActions(settings.out, prefix);
}

void FilterStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Filter Column", filter_column_name);
    map.add("Removes Filter", remove_filter_column);

    auto expression = std::make_shared<ExpressionActions>(actions_dag.clone());
    map.add("Expression", expression->toTree());
}

void FilterStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),
        FilterTransform::transformHeader(input_streams.front().header, &actions_dag, filter_column_name, remove_filter_column),
        getDataStreamTraits());

    if (!getDataStreamTraits().preserves_sorting)
        return;
}

void FilterStep::serialize(Serialization & ctx) const
{
    UInt8 flags = 0;
    if (remove_filter_column)
        flags |= 1;
    writeIntBinary(flags, ctx.out);

    writeStringBinary(filter_column_name, ctx.out);

    actions_dag.serialize(ctx.out, ctx.registry);
}

std::unique_ptr<IQueryPlanStep> FilterStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_streams.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "FilterStep must have one input stream");

    UInt8 flags;
    readIntBinary(flags, ctx.in);

    bool remove_filter_column = bool(flags & 1);

    String filter_column_name;
    readStringBinary(filter_column_name, ctx.in);

    ActionsDAG actions_dag = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context);

    return std::make_unique<FilterStep>(ctx.input_streams.front(), std::move(actions_dag), std::move(filter_column_name), remove_filter_column);
}

void registerFilterStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Filter", FilterStep::deserialize);
}

}
