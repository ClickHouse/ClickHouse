#include <Processors/Transforms/CachingTransform.h>

namespace DB
{

void CachingTransform::transform(Chunk & chunk)
{
    cache[query_ptr->getTreeHash()].first = header;
    cache[query_ptr->getTreeHash()].second.push_back(chunk.clone());
}

};
