#include <Processors/Transforms/CachingTransform.h>

namespace DB
{

void CachingTransform::transform(Chunk & chunk)
{
    LOG_DEBUG(&Poco::Logger::get("CachingTransform::transform"), "adding chunk ...");
    cache->addChunk(cache_key, chunk.clone());
    LOG_DEBUG(&Poco::Logger::get("CachingTransform::transform"), "added chunk");
}

};
