#include <IO/UncompressedCache.h>
#include <Common/SipHash.h>

namespace DB
{
template class CacheBase<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>;

UncompressedCache::UncompressedCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : Base(cache_policy, max_size_in_bytes, 0, size_ratio)
{
}

UInt128 UncompressedCache::hash(const String & path_to_file, size_t offset)
{
    SipHash hash;
    hash.update(path_to_file.data(), path_to_file.size() + 1);
    hash.update(offset);

    return hash.get128();
}
}
