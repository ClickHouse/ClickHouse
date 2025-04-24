#include <IO/UncompressedCache.h>

namespace DB
{
template class CacheBase<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>;

UncompressedCache::UncompressedCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : Base(cache_policy, max_size_in_bytes, 0, size_ratio)
{
}
}
