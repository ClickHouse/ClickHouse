#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class QueryResultCacheWriter;
class QueryResultCacheHerdTokenHolder;

class StreamInQueryResultCacheStep : public ITransformingStep
{
public:
    /// `herd_token_holder_`, if non-null, is released once the subquery's result has been fully streamed into the
    /// query result cache (or the pipeline is torn down without that happening, e.g. due to an exception or
    /// cancellation - see StreamInQueryResultCacheTransform). Used only for subqueries planned in the Planner;
    /// top-level queries manage their herd token's lifetime in executeQuery() directly.
    StreamInQueryResultCacheStep(
        const SharedHeader & input_header_,
        std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer,
        std::shared_ptr<QueryResultCacheHerdTokenHolder> herd_token_holder_ = nullptr);

    String getName() const override { return "StreamInQueryResultCache"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer;
    std::shared_ptr<QueryResultCacheHerdTokenHolder> herd_token_holder;
};

}
