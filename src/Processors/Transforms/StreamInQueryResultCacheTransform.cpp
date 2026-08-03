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

    /// The cache entry is in place (or will never be), so the queries coalescing on this computation can stop waiting.
    /// Doing it here rather than in the destructor wakes them as soon as the entry is readable instead of when the
    /// pipeline is torn down. `finish` is idempotent, so the destructor's call is a no-op afterwards.
    if (herd_token_holder)
        herd_token_holder->finish();
}

};
