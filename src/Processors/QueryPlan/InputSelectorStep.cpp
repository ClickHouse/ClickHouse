#include <Common/Exception.h>
#include <Processors/QueryPlan/InputSelectorStep.h>
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

QueryPipelineBuilderPtr InputSelectorStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 3)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "InputSelectorStep expected 3 pipelines, got {}", pipelines.size());

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
