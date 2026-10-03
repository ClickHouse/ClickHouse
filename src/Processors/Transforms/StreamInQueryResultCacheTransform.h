#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Cache/QueryResultCache.h>

namespace DB
{

class StreamInQueryResultCacheTransform final : public ISimpleTransform
{
public:
    /// `herd_token_holder_`, if non-null, is released in finalizeWriteInQueryResultCache() (the normal
    /// completion path) or, failing that, whenever this transform is eventually destroyed (e.g. the pipeline was
    /// torn down due to an exception or cancellation before finalizeWriteInQueryResultCache() was ever called) -
    /// see QueryResultCacheHerdTokenHolder's destructor. Multiple transforms (Main/Totals/Extremes) may share the
    /// same holder, exactly as they already share the same query_result_cache_writer; release() is idempotent.
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
    const std::shared_ptr<QueryResultCacheHerdTokenHolder> herd_token_holder;
};

}
