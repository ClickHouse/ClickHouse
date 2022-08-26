#include <Common/IFileCache.h>

namespace DB
{

using CacheKeyAndOffset = std::pair<IFileCache::Key, size_t>;
struct CacheKeyAndOffsetHash
{
    std::size_t operator()(const CacheKeyAndOffset & key) const
    {
        return std::hash<UInt128>()(key.first.key) ^ std::hash<UInt64>()(key.second);
    }
};

}
