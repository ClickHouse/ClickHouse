#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Sources/NullSource.h>
#include <Interpreters/Context.h>

namespace DB
{

UnionStep::UnionStep(DataStreams input_streams_, Block result_header, size_t max_threads_)
    : header(std::move(result_header))
    , max_threads(max_threads_)
{
    input_streams = std::move(input_streams_);

    if (input_streams.size() == 1)
        output_stream = input_streams.front();
    else
        output_stream = DataStream{.header = header};
}

QueryPipelinePtr UnionStep::updatePipeline(QueryPipelines pipelines)
{
    auto pipeline = std::make_unique<QueryPipeline>();
    if (pipelines.empty())
    {
        pipeline->init(Pipe(std::make_shared<NullSource>(output_stream->header)));
        return pipeline;
    }

    size_t num_pipelines = pipelines.size();
    pipeline->unitePipelines(std::move(pipelines), output_stream->header);

    if (num_pipelines > 1)
    {
        // nested queries can force 1 thread (due to simplicity)
        // but in case of union this cannot be done.
        pipeline->setMaxThreads(std::min<UInt64>(num_pipelines, max_threads));
    }

    return pipeline;
}

}
