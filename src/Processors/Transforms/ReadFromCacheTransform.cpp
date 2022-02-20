#include <Processors/Transforms/ReadFromCacheTransform.h>

namespace DB
{

Chunk ReadFromCacheTransform::generate()
{
    auto query_cache = cache[query_ptr->getTreeHash()];
    if (chunks_read_count < query_cache.size()) {
        // FIXME: find a way to return chunk from cache without invalidating it for further usage
        return std::move(*query_cache[chunks_read_count++]);
    }
}

ReadFromCacheTransform::ReadFromCacheTransform(const Block & header_, std::unordered_map<IAST::Hash, Data> & cache_, ASTPtr query_ptr_)
    : ISource(header_)
    , cache(cache_)
    , query_ptr(query_ptr_)
    , chunks_read_count(0)
{}

};
