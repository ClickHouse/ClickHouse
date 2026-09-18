#include <Processors/QueryPlan/PromQLNativeTagsBarrierStep.h>

#include <Core/Block.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PromQLNativeTagsBarrierStep::PromQLNativeTagsBarrierStep(
    SharedHeader main_header,
    SharedHeader tags_header,
    CollectorPtr collector_,
    TagsExtractor tags_extractor_)
    : collector(std::move(collector_))
    , tags_extractor(std::move(tags_extractor_))
{
    if (!main_header || !tags_header)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native tags barrier requires two input headers");
    if (!collector)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native tags barrier requires a tags collector");
    if (!tags_extractor)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native tags barrier requires a tags extractor");

    input_headers = {std::move(main_header), std::move(tags_header)};
    output_header = input_headers.front();
}

QueryPipelineBuilderPtr PromQLNativeTagsBarrierStep::updatePipeline(
    QueryPipelineBuilders pipelines,
    const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PromQL native tags barrier requires exactly two input pipelines, got {}",
            pipelines.size());

    auto main_pipeline = std::move(pipelines[0]);
    auto tags_pipeline = std::move(pipelines[1]);
    if (!main_pipeline || !tags_pipeline)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native tags barrier received a null input pipeline");

    /// Collapse the tags side before attaching the single stateful transform.
    /// Multiple independent transforms cannot safely publish one shared
    /// collector because one stream could reach EOF while another is pending.
    QueryPipelineProcessorsCollector tags_processors(*tags_pipeline, this);
    tags_pipeline->dropTotalsAndExtremes();
    tags_pipeline->resize(1);
    tags_pipeline->addSimpleTransform(
        [collector_ptr = collector, extractor = tags_extractor](const SharedHeader & header)
        {
            return std::make_shared<PromQLNativeTagsBarrierTransform>(
                header, collector_ptr, extractor);
        });
    auto added_tags_processors = tags_processors.detachProcessors();
    processors.insert(processors.end(), added_tags_processors.begin(), added_tags_processors.end());

    /// addPipelineBefore uses DelayedPortsProcessor to finish every empty
    /// tags-side stream before allowing any main output port to request data.
    QueryPipelineProcessorsCollector main_processors(*main_pipeline, this);
    main_pipeline->addPipelineBefore(std::move(*tags_pipeline));
    auto added_main_processors = main_processors.detachProcessors();
    processors.insert(processors.end(), added_main_processors.begin(), added_main_processors.end());

    return main_pipeline;
}

void PromQLNativeTagsBarrierStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
