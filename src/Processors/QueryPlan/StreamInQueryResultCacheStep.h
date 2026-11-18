#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class QueryResultCacheWriter;
class QueryResultCacheHerdTokenHolder;

class StreamInQueryResultCacheStep : public ITransformingStep
{
public:
    /// `herd_token_holder_`, if non-null, is released once all streams of the subquery are finished and its result has been
    /// written into the query result cache, even if the enclosing query is still running (or once the pipeline is torn
    /// down without that happening, e.g. due to an exception or cancellation). Used only for subqueries planned in the Planner;
    /// top-level queries manage their herd token's lifetime in executeQuery() directly.
    StreamInQueryResultCacheStep(
        const SharedHeader & input_header_,
        std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer,
        std::shared_ptr<QueryResultCacheHerdTokenHolder> herd_token_holder_ = nullptr);

    [[nodiscard]] String getName() const override { return "StreamInQueryResultCache"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    /// The clone gets its own copy of the writer: same cache key and limits, but a fresh buffer.
    /// Sharing one writer would let the original and the cloned plan buffer the same rows twice into
    /// one entry, so a cached result could contain duplicates. A copy is also what
    /// `FutureSetFromSubquery::buildOrderedSetInplace` needs: it clones the `IN` subquery source and
    /// finalizes the write only once the speculative run produced a complete set, so an aborted run
    /// caches nothing while a successful one still populates the cache — as it did when this step
    /// was not clonable at all and the source was consumed in place.
    [[nodiscard]] QueryPlanStepPtr clone() const override;

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer;
    std::shared_ptr<QueryResultCacheHerdTokenHolder> herd_token_holder;
};

}
