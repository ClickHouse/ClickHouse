#include <QueryCoordination/Pipelines/Pipelines.h>
#include <QueryCoordination/Pipelines/CompletedPipelinesExecutor.h>

namespace DB
{

void Pipelines::assignThreadNum()
{
    std::vector<Float64> threads_weight;
    Float64 total_weight = 0;

    for (const auto & query_pipeline : sources_pipelines)
    {
        Float64 weight = query_pipeline.pipeline.getProcessors().size();
        total_weight += weight;
        threads_weight.emplace_back(weight);
    }

    if (root_pipeline.pipeline.initialized())
    {
        Float64 weight = root_pipeline.pipeline.getProcessors().size();
        total_weight += weight;
        threads_weight.emplace_back(weight);
    }

    for (size_t i = 0; i < threads_weight.size(); ++i)
    {
        if (root_pipeline.pipeline.initialized() && (i == threads_weight.size() - 1))
        {
            size_t num_threads = static_cast<size_t>((threads_weight[i] / total_weight) * max_threads);
            root_pipeline.pipeline.setNumThreads(num_threads);
        }
        else
        {
            size_t num_threads = static_cast<size_t>((threads_weight[i] / total_weight) * max_threads);
            sources_pipelines[i].pipeline.setNumThreads(num_threads);
        }
    }
}

std::shared_ptr<QueryCoordinationExecutor> Pipelines::createPipelinesExecutor()
{
//    LOG_DEBUG(log, "Create pipelines executor for query {}", query_id);

    if (!sources_pipelines.empty())
    {
        std::vector<Int32> fragment_ids;
        std::vector<QueryPipeline> pipelines;
        for (auto & query_pipeline : sources_pipelines)
        {
            pipelines.emplace_back(std::move(query_pipeline.pipeline));
            fragment_ids.emplace_back(query_pipeline.fragment_id);
        }

        auto completed_pipelines_executor = std::make_shared<CompletedPipelinesExecutor>(pipelines, fragment_ids);
    }

    std::shared_ptr<PullingAsyncPipelineExecutor> pulling_executor = std::make_shared<PullingAsyncPipelineExecutor>(root_pipeline.pipeline);

    auto remote_pipelines_manager = std::make_shared<RemotePipelinesManager>();
    /// TODO set nodes

    return std::make_shared<QueryCoordinationExecutor>(pulling_executor, completed_pipelines_executor, remote_pipelines_manager);
}

std::shared_ptr<CompletedPipelinesExecutor> Pipelines::createCompletedPipelinesExecutor()
{
    //    LOG_DEBUG(log, "Create pipelines executor for query {}", query_id);

    std::vector<Int32> fragment_ids;
    std::vector<QueryPipeline> pipelines;
    for (auto & query_pipeline : sources_pipelines)
    {
        pipelines.emplace_back(std::move(query_pipeline.pipeline));
        fragment_ids.emplace_back(query_pipeline.fragment_id);
    }

    return std::make_shared<CompletedPipelinesExecutor>(pipelines, fragment_ids);
}

}
