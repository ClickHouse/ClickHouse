#include <Columns/IColumn.h>
#include <Common/SipHash.h>
#include <Common/CurrentMetrics.h>
#include <Storages/MergeTree/PrimaryIndexCache.h>

namespace CurrentMetrics
{
    extern const Metric PrimaryIndexCacheBytes;
    extern const Metric PrimaryIndexCacheFiles;
}

namespace DB
{

size_t PrimaryIndexWeightFunction::operator()(const PrimaryIndex & index) const
{
    size_t res = PRIMARY_INDEX_CACHE_OVERHEAD;
    res += index.capacity() * sizeof(PrimaryIndex::value_type);
    for (const auto & column : index)
        res += column->allocatedBytes();
    return res;
}

template class CacheBase<UInt128, PrimaryIndex, UInt128TrivialHash, PrimaryIndexWeightFunction>;


PrimaryIndexCache::PrimaryIndexCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : Base(cache_policy, CurrentMetrics::PrimaryIndexCacheBytes, CurrentMetrics::PrimaryIndexCacheFiles, max_size_in_bytes, 0, size_ratio)
{
}

UInt128 PrimaryIndexCache::hash(const String & part_path)
{
    SipHash hash;
    hash.update(part_path.data(), part_path.size() + 1);
    return hash.get128();
}

}
