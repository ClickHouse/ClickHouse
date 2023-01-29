#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Cache/QueryResultCache.h>

namespace DB
{

class StreamInQueryResultCacheTransform : public ISimpleTransform
{
public:
    StreamInQueryResultCacheTransform(
        const Block & header_, QueryResultCachePtr cache, const QueryResultCache::Key & cache_key, std::chrono::milliseconds min_query_duration);

protected:
    void transform(Chunk & chunk) override;

public:
    void finalizeWriteInQueryResultCache();
    String getName() const override { return "StreamInQueryResultCacheTransform"; }

private:
    QueryResultCache::Writer cache_writer;
};

}
