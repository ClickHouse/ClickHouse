#include <Interpreters/Context.h>
#include <Processors/QueryPipeline.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/IntersectOrExceptTransform.h>
#include <Processors/ResizeProcessor.h>

namespace DB
{

IntersectOrExceptStep::IntersectOrExceptStep(bool is_except_, DataStreams input_streams_, Block result_header, size_t max_threads_)
    : is_except(is_except_), header(std::move(result_header)), max_threads(max_threads_)
{
    input_streams = std::move(input_streams_);
    output_stream = DataStream{.header = header};
}

QueryPipelinePtr IntersectOrExceptStep::updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & )
{
    auto pipeline = std::make_unique<QueryPipeline>();
    QueryPipelineProcessorsCollector collector(*pipeline, this);

    pipelines[0]->addTransform(std::make_shared<ResizeProcessor>(header, pipelines[0]->getNumStreams(), 1));
    pipelines[1]->addTransform(std::make_shared<ResizeProcessor>(header, pipelines[1]->getNumStreams(), 1));

    *pipeline = QueryPipeline::unitePipelines(std::move(pipelines), max_threads);
    pipeline->addTransform(std::make_shared<IntersectOrExceptTransform>(is_except, header));

    processors = collector.detachProcessors();
    return pipeline;
}

void IntersectOrExceptStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
