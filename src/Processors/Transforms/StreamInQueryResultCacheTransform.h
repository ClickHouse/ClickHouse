#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Cache/QueryResultCache.h>

namespace DB
{

class StreamInQueryResultCacheTransform final : public ISimpleTransform
{
public:
    StreamInQueryResultCacheTransform(
        const Block & header_,
        std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer,
        QueryResultCacheWriter::ChunkType chunk_type,
        std::shared_ptr<QueryResultCacheHerdTokenHolder> herd_token_holder = nullptr);

protected:
    void transform(Chunk & chunk) override;

public:
    void finalizeWriteInQueryResultCache();
    String getName() const override { return "StreamInQueryResultCacheTransform"; }

private:
    const std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer;
    const QueryResultCacheWriter::ChunkType chunk_type;
    /// Only set on the Planner-level subquery path. Wakes the queries waiting for this computation, either right after
    /// the result was written to the cache or, if that never happens (exception, cancellation, plan never executed),
    /// when the transform is destroyed.
    const std::shared_ptr<QueryResultCacheHerdTokenHolder> herd_token_holder;
};

}
