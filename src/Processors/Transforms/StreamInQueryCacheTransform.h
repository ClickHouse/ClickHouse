#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Cache/QueryCache.h>

namespace DB
{

class StreamInQueryCacheTransform : public ISimpleTransform
{
public:
    StreamInQueryCacheTransform(
        const Block & header_, QueryCachePtr cache, const QueryCache::Key & cache_key, std::chrono::milliseconds min_query_duration);

protected:
    void transform(Chunk & chunk) override;

public:
    void finalizeWriteInQueryCache();
    String getName() const override { return "StreamInQueryCacheTransform"; }

private:
    QueryCache::Writer cache_writer;
};

}
