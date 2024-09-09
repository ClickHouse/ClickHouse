#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/Transforms/FilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits(const ActionsDAGPtr & expression, const Block & header, const SortDescription & sort_description, bool remove_filter_column, const String & filter_column_name)
{
    bool preserves_sorting = expression->isSortingPreserved(header, sort_description, remove_filter_column ? filter_column_name : "");
    if (remove_filter_column)
    {
        preserves_sorting &= std::find_if(
                                 begin(sort_description),
                                 end(sort_description),
                                 [&](const auto & column_desc) { return column_desc.column_name == filter_column_name; })
            == sort_description.end();
    }
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = preserves_sorting,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FilterStep::FilterStep(
    const DataStream & input_stream_,
    const ActionsDAGPtr & actions_dag_,
    String filter_column_name_,
    bool remove_filter_column_)
    : ITransformingStep(
        input_stream_,
        FilterTransform::transformHeader(
            input_stream_.header,
            actions_dag_.get(),
            filter_column_name_,
            remove_filter_column_),
        getTraits(actions_dag_, input_stream_.header, input_stream_.sort_description, remove_filter_column_, filter_column_name_))
    , actions_dag(actions_dag_)
    , filter_column_name(std::move(filter_column_name_))
    , remove_filter_column(remove_filter_column_)
{
    actions_dag = actions_dag->clone();
    actions_dag->removeAliasesForFilter(filter_column_name);
}

void FilterStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto expression = std::make_shared<ExpressionActions>(actions_dag, settings.getActionsSettings());

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
        auto convert_actions = std::make_shared<ExpressionActions>(convert_actions_dag, settings.getActionsSettings());

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

    auto expression = std::make_shared<ExpressionActions>(actions_dag);
    expression->describeActions(settings.out, prefix);
}

void FilterStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Filter Column", filter_column_name);
    map.add("Removes Filter", remove_filter_column);

    auto expression = std::make_shared<ExpressionActions>(actions_dag);
    map.add("Expression", expression->toTree());
}

void FilterStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),
        FilterTransform::transformHeader(input_streams.front().header, actions_dag.get(), filter_column_name, remove_filter_column),
        getDataStreamTraits());

    if (!getDataStreamTraits().preserves_sorting)
        return;

    FindAliasForInputName alias_finder(actions_dag);
    const auto & input_sort_description = getInputStreams().front().sort_description;
    for (size_t i = 0, s = input_sort_description.size(); i < s; ++i)
    {
        const auto & original_column = input_sort_description[i].column_name;
        const auto * alias_node = alias_finder.find(original_column);
        if (alias_node)
            output_stream->sort_description[i].column_name = alias_node->result_name;
    }
}

}
