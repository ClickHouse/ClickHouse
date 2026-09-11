#include <Processors/Transforms/StreamInQueryResultCacheTransform.h>

namespace DB
{

StreamInQueryResultCacheTransform::StreamInQueryResultCacheTransform(
    const Block & header_,
    std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer_,
    QueryResultCacheWriter::ChunkType chunk_type_,
    std::shared_ptr<QueryResultCacheHerdTokenHolder> herd_token_holder_)
    : ISimpleTransform(header_, header_, false)
    , query_result_cache_writer(query_result_cache_writer_)
    , chunk_type(chunk_type_)
    , herd_token_holder(std::move(herd_token_holder_))
{
}

void StreamInQueryResultCacheTransform::transform(Chunk & chunk)
{
    compactReplicatedColumns(chunk);
    query_result_cache_writer->buffer(chunk.clone(), chunk_type);
}

void StreamInQueryResultCacheTransform::finalizeWriteInQueryResultCache()
{
    if (!isCancelled())
        query_result_cache_writer->finalizeWrite();

    /// Release regardless of cancellation: the subquery's execution (successful or not) is over either way, so
    /// any query waiting on this herd token should stop waiting and re-probe the cache.
    if (herd_token_holder)
        herd_token_holder->release();
}

};
