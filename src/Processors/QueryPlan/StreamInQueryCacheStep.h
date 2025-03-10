#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class QueryResultCacheWriter;

class StreamInQueryResultCacheStep : public ITransformingStep
{
public:
    StreamInQueryResultCacheStep(const Header & input_header_, std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer);

    String getName() const override { return "StreamInQueryResultCache"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer;
};

}
