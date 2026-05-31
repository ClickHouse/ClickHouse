#include <Processors/QueryPlan/Streaming/WatermarkMergerStep.h>
#include <Processors/Streaming/WatermarkMerger.h>
#include <Processors/Port.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

SharedHeader checkHeaders(const SharedHeaders & input_headers)
{
    if (input_headers.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "WatermarkMergerStep cannot be built without input plans");

    auto res = input_headers.front();
    for (const auto & header : input_headers)
        assertBlocksHaveEqualStructure(*header, *res, "WatermarkMergerStep");

    return res;
}

}

WatermarkMergerStep::WatermarkMergerStep(SharedHeaders input_headers_, size_t num_output_streams_)
    : num_output_streams(num_output_streams_)
{
    updateInputHeaders(std::move(input_headers_));
}

void WatermarkMergerStep::updateOutputHeader()
{
    output_header = checkHeaders(input_headers);
}

QueryPipelineBuilderPtr WatermarkMergerStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    auto pipeline = std::make_unique<QueryPipelineBuilder>(QueryPipelineBuilder::unitePipelines(std::move(pipelines), settings.max_threads, &processors));

    QueryPipelineProcessorsCollector collector(*pipeline, this);
    pipeline->transform([header = pipeline->getSharedHeader(), num_outputs = num_output_streams](OutputPortRawPtrs ports) -> Processors
    {
        auto merger = std::make_shared<WatermarkMerger>(header, ports.size(), num_outputs);

        for (const auto [output, input] : std::views::zip(ports, merger->getInputs()))
            connect(*output, input);

        return {merger};
    });

    processors.append_range(collector.detachProcessors());
    return pipeline;
}

void WatermarkMergerStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
