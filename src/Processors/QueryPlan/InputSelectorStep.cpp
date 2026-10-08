#include <Common/Exception.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/InputSelectorStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

InputSelectorStep::InputSelectorStep(SharedHeader signal_header, SharedHeader data_header)
{
    SharedHeaders headers;
    headers.emplace_back(std::move(signal_header));
    headers.emplace_back(data_header);
    headers.emplace_back(data_header);
    updateInputHeaders(std::move(headers));
}

void InputSelectorStep::updateOutputHeader()
{
    if (!input_headers[0]->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "InputSelectorStep: signal header must be empty");

    assertBlocksHaveEqualStructure(*input_headers[1], *input_headers[2], "InputSelectorStep");

    output_header = input_headers[1];
}

/// If the pipeline header has extra columns not in the expected header, add an ExpressionTransform to drop them.
static void dropExtraColumns(QueryPipelineBuilder & pipeline, const Block & expected_header)
{
    const auto & actual_header = pipeline.getHeader();
    if (blocksHaveEqualStructure(actual_header, expected_header))
        return;

    auto converting_dag = ActionsDAG::makeConvertingActions(
        actual_header.getColumnsWithTypeAndName(),
        expected_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        nullptr);

    auto expression = std::make_shared<ExpressionActions>(std::move(converting_dag));

    pipeline.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<ExpressionTransform>(header, expression);
    });
}

QueryPipelineBuilderPtr InputSelectorStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 3)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "InputSelectorStep expected 3 pipelines, got {}", pipelines.size());

    /// Pipeline headers may differ from plan headers (e.g. ReadFromMergeTree with deferred
    /// row_level_filter adds extra columns to the pipeline). Strip them to match the expected header.
    const auto & expected = *getOutputHeader();
    dropExtraColumns(*pipelines[1], expected);
    dropExtraColumns(*pipelines[2], expected);

    return QueryPipelineBuilder::selectPipeline(
        std::move(pipelines[0]),
        std::move(pipelines[1]),
        std::move(pipelines[2]),
        &processors);
}

void InputSelectorStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
