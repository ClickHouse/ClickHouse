#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ISourceStep::ISourceStep(Header output_header_)
{
    output_header = std::move(output_header_);
}

QueryPipelineBuilderPtr ISourceStep::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings & settings)
{
    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    /// For `Source` step, since it's not add new Processors to `pipeline->pipe`
    /// in `initializePipeline`, but make an assign with new created Pipe.
    /// And Processors for the Step is added here. So we do not need to use
    /// `QueryPipelineProcessorsCollector` to collect Processors.
    initializePipeline(*pipeline, settings);

    /// But we need to set QueryPlanStep manually for the Processors, which
    /// will be used in `EXPLAIN PIPELINE`
    for (auto & processor : processors)
    {
        processor->setQueryPlanStep(this);
    }
    return pipeline;
}

void ISourceStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
