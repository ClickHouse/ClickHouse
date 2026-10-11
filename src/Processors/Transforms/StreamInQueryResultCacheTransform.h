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
        QueryResultCacheWriter::ChunkType chunk_type);

protected:
    void transform(Chunk & chunk) override;
    Status prepare() override;

public:
    void finalizeWriteInQueryResultCache();
    String getName() const override { return "StreamInQueryResultCacheTransform"; }

private:
    const std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer;
    const QueryResultCacheWriter::ChunkType chunk_type;

    /// Whether the upstream finished and all its chunks were buffered into the writer.
    bool input_exhausted = false;
    bool finished = false;
};

}
