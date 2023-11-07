#pragma once

#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

class QueryCoordinationExecutor;
class CompletedPipelinesExecutor;

class Pipelines
{
public:
    struct FragmentIDPipeline
    {
        Int32 fragment_id = 0;
        QueryPipeline pipeline;
    };

    void assignThreadNum(size_t max_threads_);

    QueryPipeline detachRootPipeline() { return std::move(root_pipeline.pipeline); }

    std::shared_ptr<QueryCoordinationExecutor>
    createCoordinationExecutor(QueryPipeline & pipeline, const StorageLimitsList & storage_limits_);

    std::shared_ptr<CompletedPipelinesExecutor> createCompletedPipelinesExecutor();

    void addRootPipeline(Int32 fragment_id, QueryPipeline root_pipeline_)
    {
        root_pipeline = FragmentIDPipeline{.fragment_id = fragment_id, .pipeline = std::move(root_pipeline_)};
    }

    void addSourcesPipeline(Int32 fragment_id, QueryPipeline sources_pipeline)
    {
        sources_pipelines.emplace_back(FragmentIDPipeline{.fragment_id = fragment_id, .pipeline = std::move(sources_pipeline)});
    }

private:
    FragmentIDPipeline root_pipeline;
    std::vector<FragmentIDPipeline> sources_pipelines;
    size_t max_threads;
};

}
