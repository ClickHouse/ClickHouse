#pragma once
#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/HashTable/Hash.h>
#include <Columns/IColumn_fwd.h>

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

    size_t operator()(const PrimaryIndex & index) const;
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
    PrimaryIndexCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & part_path);

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
