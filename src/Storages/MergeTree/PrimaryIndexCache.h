#pragma once
#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/HashTable/Hash.h>
#include <Columns/IColumn.h>

namespace ProfileEvents
{
    extern const Event PrimaryIndexCacheHits;
    extern const Event PrimaryIndexCacheMisses;
}

namespace DB
{

using PrimaryIndex = std::vector<ColumnPtr>;

/// Estimate of number of bytes in cache for primary index.
struct PrimaryIndexWeightFunction
{
    /// We spent additional bytes on key in hashmap, linked lists, shared pointers, etc ...
    static constexpr size_t PRIMARY_INDEX_CACHE_OVERHEAD = 128;

    size_t operator()(const PrimaryIndex & index) const
    {
        size_t res = PRIMARY_INDEX_CACHE_OVERHEAD;
        res += index.capacity() * sizeof(PrimaryIndex::value_type);
        for (const auto & column : index)
            res += column->allocatedBytes();
        return res;
    }
};

extern template class CacheBase<UInt128, PrimaryIndex, UInt128TrivialHash, PrimaryIndexWeightFunction>;

/** Cache of primary index for MergeTree tables.
  * Primary index is a list of columns from primary key
  * that store first row for each granule of data part.
  */
class PrimaryIndexCache : public CacheBase<UInt128, PrimaryIndex, UInt128TrivialHash, PrimaryIndexWeightFunction>
{
private:
    using Base = CacheBase<UInt128, PrimaryIndex, UInt128TrivialHash, PrimaryIndexWeightFunction>;

public:
    PrimaryIndexCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
        : Base(cache_policy, max_size_in_bytes, 0, size_ratio)
    {
    }

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & part_path)
    {
        SipHash hash;
        hash.update(part_path.data(), part_path.size() + 1);
        return hash.get128();
    }

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::PrimaryIndexCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::PrimaryIndexCacheHits);

        return result.first;
    }
};

using PrimaryIndexCachePtr = std::shared_ptr<PrimaryIndexCache>;

}
