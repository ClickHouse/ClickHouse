#include <IO/UncompressedCache.h>
#include <Common/SipHash.h>

#include <algorithm>

namespace DB
{
template class CacheBase<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>;

UncompressedCache::UncompressedCache(const String & cache_policy,
    CurrentMetrics::Metric size_in_bytes_metric,
    CurrentMetrics::Metric count_metric,
    size_t max_size_in_bytes,
    double size_ratio)
    : Base(cache_policy, size_in_bytes_metric, count_metric, max_size_in_bytes, 0, size_ratio)
{
}

UInt128 UncompressedCache::hash(const String & path_to_file, size_t offset)
{
    SipHash hash;
    hash.update(path_to_file.size());
    hash.update(path_to_file.data(), path_to_file.size());
    hash.update(offset);
    return hash.get128();
}

void UncompressedCache::removeEntriesUnderPaths(const Strings & prefixes)
{
    std::function<bool(const Key &, const MappedPtr &)> predicate = [&](const Key &, const MappedPtr & cell)
    {
        return std::ranges::any_of(prefixes, [&](const String & prefix) { return cell->path.starts_with(prefix); });
    };
    Base::remove(predicate);
}
}
