#include <Common/SipHash.h>
#include <Common/CurrentMetrics.h>
#include <Storages/MarkCache.h>

namespace CurrentMetrics
{
    extern const Metric MarkCacheBytes;
    extern const Metric MarkCacheFiles;
}

namespace DB
{
template class CacheBase<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>;

MarkCache::MarkCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : Base(cache_policy, CurrentMetrics::MarkCacheBytes, CurrentMetrics::MarkCacheFiles, max_size_in_bytes, 0, size_ratio)
{
}

UInt128 MarkCache::hash(const String& path_to_file)
{
    SipHash hash;
    hash.update(path_to_file.data(), path_to_file.size() + 1);
    return hash.get128();
}


}
