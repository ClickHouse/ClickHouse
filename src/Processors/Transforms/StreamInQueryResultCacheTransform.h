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

    String getName() const override { return "StreamInQueryResultCacheTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    QueryResultCache::Writer cache_writer;
};

}
