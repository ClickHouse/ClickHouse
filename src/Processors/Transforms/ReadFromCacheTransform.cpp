#include <Processors/Transforms/ReadFromCacheTransform.h>

namespace DB
{

Chunk ReadFromCacheTransform::generate()
{
    auto query_cache = cache[query_ptr->getTreeHash()];
    if (chunks_read_count < query_cache.size()) {
        const auto &chunk = query_cache[chunks_read_count++];
        return Chunk(chunk.getColumns(), chunk.getNumRows(), chunk.getChunkInfo());
    }
    return {};
}

ReadFromCacheTransform::ReadFromCacheTransform(const Block & header_, std::unordered_map<IAST::Hash, Data, ASTHash> & cache_, ASTPtr query_ptr_)
    : ISource(header_)
    , cache(cache_)
    , query_ptr(query_ptr_)
    , chunks_read_count(0)
{}

};
