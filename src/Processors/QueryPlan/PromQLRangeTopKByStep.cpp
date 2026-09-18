#include <Processors/QueryPlan/PromQLRangeTopKByStep.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

}

PromQLRangeTopKByStep::PromQLRangeTopKByStep(SharedHeader input_header_, UInt64 k_, bool bottomk_)
    : ITransformingStep(input_header_, PromQLRangeTopKByTransform::transformHeader(input_header_), getTraits())
    , k(k_)
    , bottomk(bottomk_)
{
}

void PromQLRangeTopKByStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.dropTotalsAndExtremes();
    if (pipeline.getNumStreams() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PromQL native range topk requires one already merged group/value stream, got {}",
            pipeline.getNumStreams());

    pipeline.addSimpleTransform(
        [limit = this->k, is_bottomk = this->bottomk](const SharedHeader & header)
        { return std::make_shared<PromQLRangeTopKByTransform>(header, limit, is_bottomk); });
}

void PromQLRangeTopKByStep::updateOutputHeader()
{
    output_header = PromQLRangeTopKByTransform::transformHeader(input_headers.front());
}

}
