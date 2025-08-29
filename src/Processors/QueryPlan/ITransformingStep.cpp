#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ITransformingStep::ITransformingStep(SharedHeader input_header, SharedHeader output_header_, Traits traits, bool collect_processors_)
    : transform_traits(std::move(traits.transform_traits))
    , collect_processors(collect_processors_)
    , data_stream_traits(std::move(traits.data_stream_traits))
{
    input_headers.emplace_back(std::move(input_header));
    output_header = std::move(output_header_);
}

QueryPipelineBuilderPtr ITransformingStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    if (collect_processors)
    {
        QueryPipelineProcessorsCollector collector(*pipelines.front(), this);
        transformPipeline(*pipelines.front(), settings);
        processors = collector.detachProcessors();
    }
    else
        transformPipeline(*pipelines.front(), settings);

    return std::move(pipelines.front());
}

void ITransformingStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

[[noreturn]] inline void throwNotImplemented()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

void ITransformingStep::adjustSettingsToEnforceSortingPropertiesInDistributedQuery(ContextMutablePtr) const { throwNotImplemented(); }

}
