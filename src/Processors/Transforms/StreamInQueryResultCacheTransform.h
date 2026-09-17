#pragma once

#include <mutex>

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
        QueryResultCacheWriter::ChunkType chunk_type);

protected:
    void transform(Chunk & chunk) override;

public:
    void finalizeWriteInQueryResultCache();
    void onPartialResult() noexcept override;

    String getName() const override { return "StreamInQueryResultCacheTransform"; }

private:
    std::mutex cache_publication_mutex;
    bool partial_result = false;
    const std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer;
    const QueryResultCacheWriter::ChunkType chunk_type;
};

}
