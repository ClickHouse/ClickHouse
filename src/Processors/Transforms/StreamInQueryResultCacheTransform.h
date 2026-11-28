#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Cache/QueryResultCache.h>

#include <atomic>

namespace DB
{

/// State shared by all `StreamInQueryResultCacheTransform`s of one subquery which owns a herd token (one transform per stream,
/// including the totals and extremes streams). The subquery result is complete as soon as all these streams are finished,
/// which may happen long before the enclosing query finishes (and calls `finalizeWriteInQueryResultCache`). Then the result is
/// written into the query result cache and the herd token is released right away, so that the queries waiting for the same
/// subquery don't have to wait for the rest of the enclosing query.
struct StreamInQueryResultCacheSubqueryCompletion
{
    explicit StreamInQueryResultCacheSubqueryCompletion(std::shared_ptr<QueryResultCacheHerdTokenHolder> herd_token_holder_)
        : herd_token_holder(std::move(herd_token_holder_))
    {
    }

    const std::shared_ptr<QueryResultCacheHerdTokenHolder> herd_token_holder;
    /// Number of transforms which are not finished yet. Incremented when a transform is created, i.e. while the pipeline is
    /// built, before any of them can finish.
    std::atomic<size_t> unfinished_streams{0};
    /// An exception passed through one of the streams: the buffered result must not be stored.
    std::atomic<bool> has_exception{false};
};

class StreamInQueryResultCacheTransform final : public ISimpleTransform
{
public:
    /// `subquery_completion_`, if non-null, owns the herd token of a subquery planned in the Planner (top-level queries
    /// manage their herd token in `executeQuery` directly). The token is released once all streams of the subquery are
    /// finished (see `StreamInQueryResultCacheSubqueryCompletion`), else in `finalizeWriteInQueryResultCache`, or, failing
    /// that, when the last transform is destroyed (e.g. the pipeline was torn down due to an exception or cancellation) -
    /// see the destructor of `QueryResultCacheHerdTokenHolder`, which tells the waiters that the execution failed.
    StreamInQueryResultCacheTransform(
        const Block & header_,
        std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer,
        QueryResultCacheWriter::ChunkType chunk_type,
        std::shared_ptr<StreamInQueryResultCacheSubqueryCompletion> subquery_completion_ = nullptr);

protected:
    void transform(Chunk & chunk) override;
    void transform(std::exception_ptr & exception) override;
    void onFinish() override;
    Status prepare() override;
    void cancel(CancelReason reason) noexcept override;

public:
    void finalizeWriteInQueryResultCache();
    String getName() const override { return "StreamInQueryResultCacheTransform"; }

private:
    const std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer;
    const QueryResultCacheWriter::ChunkType chunk_type;
    const std::shared_ptr<StreamInQueryResultCacheSubqueryCompletion> subquery_completion;
    bool stream_finished = false;
};

}
