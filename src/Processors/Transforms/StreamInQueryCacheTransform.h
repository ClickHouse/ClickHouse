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
        QueryCachePtr cache,
        const QueryCache::Key & cache_key,
        std::chrono::milliseconds min_query_duration,
        bool squash_partial_results,
        size_t max_block_size,
        size_t max_query_cache_size_in_bytes_quota, size_t max_query_cache_entries_quota);

protected:
    void transform(Chunk & chunk) override;

public:
    void finalizeWriteInQueryCache();
    String getName() const override { return "StreamInQueryCacheTransform"; }

private:
    QueryCache::Writer cache_writer;
};

}
