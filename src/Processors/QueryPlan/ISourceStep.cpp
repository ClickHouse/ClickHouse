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
    initializePipeline(*pipeline, settings);
    QueryPipelineProcessorsCollector collector(*pipeline, this);

    /// Properly collecting processors from Pipe.
    /// At the creation time of a Pipe, since `collected_processors` is nullptr,
    /// the processors can not be collected.
    pipeline->collectProcessors();
    auto added_processors = collector.detachProcessors();
    processors.insert(processors.end(), added_processors.begin(), added_processors.end());
    return pipeline;
}

void ISourceStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
