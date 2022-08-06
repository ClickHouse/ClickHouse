#include <algorithm>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>
#include <Interpreters/JoinSwitcher.h>
#include <Common/JSONBuilder.h>
#include "Core/SortDescription.h"

namespace DB
{

static Poco::Logger * getLogger()
{
    static Poco::Logger & logger = Poco::Logger::get("ExpressionStep");
    return &logger;
}

static bool isSortingPreserved(const Block& header, const ActionsDAGPtr & actions, const SortDescription& sort_description)
{
    if (actions->hasArrayJoin())
        return false;

    const Block & output_header = actions->updateHeader(header);
    for (const auto & desc : sort_description)
    {
        if (!output_header.findByName(desc.column_name))
            return false;
    }

    return true;
}

static ITransformingStep::Traits getTraits(const Block& header, const ActionsDAGPtr & actions, const SortDescription& sort_description)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = !actions->hasArrayJoin(),
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = isSortingPreserved(header, actions, sort_description),
        },
        {
            .preserves_number_of_rows = !actions->hasArrayJoin(),
        }
    };
}

ExpressionStep::ExpressionStep(const DataStream & input_stream_, const ActionsDAGPtr & actions_dag_)
    : ITransformingStep(
        input_stream_,
        ExpressionTransform::transformHeader(input_stream_.header, *actions_dag_),
        getTraits(input_stream_.header, actions_dag_, input_stream_.sort_description))
    , actions_dag(actions_dag_)
{
    LOG_DEBUG(getLogger(), "ActionsDAG:\n{}", actions_dag->dumpDAG());

    /// Some columns may be removed by expression.
    updateDistinctColumns(output_stream->header, output_stream->distinct_columns);
}

void ExpressionStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto expression = std::make_shared<ExpressionActions>(actions_dag, settings.getActionsSettings());

    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExpressionTransform>(header, expression);
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

void ExpressionStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    bool first = true;

    auto expression = std::make_shared<ExpressionActions>(actions_dag);
    for (const auto & action : expression->getActions())
    {
        settings.out << prefix << (first ? "Actions: "
                                         : "         ");
        first = false;
        settings.out << action.toString() << '\n';
    }

    settings.out << prefix << "Positions:";
    for (const auto & pos : expression->getResultPositions())
        settings.out << ' ' << pos;
    settings.out << '\n';
}

void ExpressionStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto expression = std::make_shared<ExpressionActions>(actions_dag);
    map.add("Expression", expression->toTree());
}

void ExpressionStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(), ExpressionTransform::transformHeader(input_streams.front().header, *actions_dag), getDataStreamTraits());
}

}
