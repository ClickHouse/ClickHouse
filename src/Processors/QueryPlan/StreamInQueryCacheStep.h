#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class QueryCacheWriter;

class StreamInQueryCacheStep : public ITransformingStep
{
public:
    StreamInQueryCacheStep(const Header & input_header_, std::shared_ptr<QueryCacheWriter> query_cache_writer);

    String getName() const override { return "StreamInQueryCache"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    std::shared_ptr<QueryCacheWriter> query_cache_writer;
};

}
