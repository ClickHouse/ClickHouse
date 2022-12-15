#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Cache/QueryResultCache.h>

namespace DB
{

class StreamInQueryResultCacheTransform : public ISimpleTransform
{
public:
    StreamInQueryResultCacheTransform(const Block & header_, QueryResultCachePtr cache, const QueryResultCache::Key & cache_key,
        size_t max_entries, size_t max_entry_size_in_bytes, size_t max_entry_size_in_rows, std::chrono::milliseconds min_query_duration);

    String getName() const override { return "StreamInQueryResultCacheTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    QueryResultCache::Writer cache_writer;
};

}
