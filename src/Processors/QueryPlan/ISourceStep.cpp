#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ISourceStep::ISourceStep(DataStream output_stream_)
{
    output_stream = std::move(output_stream_);
}

QueryPipelineBuilderPtr ISourceStep::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings & settings)
{
    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    /// Why we need initializePipeline first: since it's not add
    /// new Processors to `pipeline->pipe`, but make an assign
    /// with new created Pipe. And Processors for the Step is added here.
    initializePipeline(*pipeline, settings);

    QueryPipelineProcessorsCollector collector(*pipeline, this);

    /// Properly collecting processors from Pipe.
    /// At the creation time of a Pipe, since `collected_processors` is nullptr,
    /// the processors can not be collected.
    pipeline->collectProcessors();
    collector.detachProcessors();
    return pipeline;
}

void ISourceStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
