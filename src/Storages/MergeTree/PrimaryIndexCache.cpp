#include <Common/SipHash.h>
#include <Storages/MergeTree/PrimaryIndexCache.h>

namespace DB
{

template class CacheBase<UInt128, PrimaryIndex, UInt128TrivialHash, PrimaryIndexWeightFunction>;


UInt128 PrimaryIndexCache::hash(const String & part_path)
{
    SipHash hash;
    hash.update(part_path.data(), part_path.size() + 1);
    return hash.get128();
}

}
