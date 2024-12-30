#include <Common/SipHash.h>
#include <Storages/MarkCache.h>

namespace DB
{
template class CacheBase<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>;

MarkCache::MarkCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : Base(cache_policy, max_size_in_bytes, 0, size_ratio)
{
}

UInt128 MarkCache::hash(const String& path_to_file)
{
    SipHash hash;
    hash.update(path_to_file.data(), path_to_file.size() + 1);
    return hash.get128();
}


}
