#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class QueryResultCacheWriter;
class QueryResultCacheHerdTokenHolder;

class StreamInQueryResultCacheStep : public ITransformingStep
{
public:
    /// `herd_token_holder` is optional and only set on the Planner-level subquery path: it keeps this query the herd
    /// "executor" for the cache key until the result has been written, so that concurrent identical subqueries wait for
    /// this computation instead of running their own. See `QueryResultCacheHerdTokenHolder`.
    StreamInQueryResultCacheStep(
        const SharedHeader & input_header_,
        std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer,
        std::shared_ptr<QueryResultCacheHerdTokenHolder> herd_token_holder = nullptr);

    String getName() const override { return "StreamInQueryResultCache"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer;
    std::shared_ptr<QueryResultCacheHerdTokenHolder> herd_token_holder;
};

}
