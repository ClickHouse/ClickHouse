#include <Processors/QueryPlan/JoinOrderAlternativesStep.h>

#include <Common/Exception.h>
#include <Core/Block.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinOrderAlternativesStep::JoinOrderAlternativesStep(SharedHeaders input_headers_)
{
    updateInputHeaders(std::move(input_headers_));
}

void JoinOrderAlternativesStep::updateOutputHeader()
{
    if (input_headers.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinOrderAlternatives expects 2 inputs, got {}", input_headers.size());
    if (!blocksHaveEqualStructure(*input_headers[0], *input_headers[1]))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinOrderAlternatives inputs have different headers: {} and {}",
            input_headers[0]->dumpStructure(), input_headers[1]->dumpStructure());
    output_header = input_headers.front();
}

QueryPipelineBuilderPtr JoinOrderAlternativesStep::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinOrderAlternatives must be resolved before the pipeline is built");
}

QueryPlanStepPtr JoinOrderAlternativesStep::clone() const
{
    return std::make_unique<JoinOrderAlternativesStep>(input_headers);
}

}
