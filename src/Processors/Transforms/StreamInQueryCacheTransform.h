#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Cache/QueryCache.h>

namespace DB
{

class StreamInQueryCacheTransform : public ISimpleTransform
{
public:
    StreamInQueryCacheTransform(
        const Block & header_,
        std::shared_ptr<QueryCache::Writer> query_cache_writer,
        QueryCache::Writer::ChunkType chunk_type);

protected:
    void transform(Chunk & chunk) override;

public:
    void finalizeWriteInQueryCache();
    String getName() const override { return "StreamInQueryCacheTransform"; }

private:
    const std::shared_ptr<QueryCache::Writer> query_cache_writer;
    const QueryCache::Writer::ChunkType chunk_type;
};

}
