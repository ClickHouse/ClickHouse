#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Cache/QueryResultCache.h>

namespace DB
{

class StreamInQueryResultCacheTransform : public ISimpleTransform
{
public:
    StreamInQueryResultCacheTransform(
        const Block & header_,
        QueryResultCachePtr cache,
        QueryResultCache::Key cache_key,
        size_t max_entries,
        size_t max_entry_size);

    String getName() const override { return "StreamInQueryResultCacheTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    QueryResultCache::Writer cache_writer;
};

}
