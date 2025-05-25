#pragma once

#include <memory>

#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/HashTable/Hash.h>
#include <Formats/MarkInCompressedFile.h>


namespace ProfileEvents
{
    extern const Event MarkCacheHits;
    extern const Event MarkCacheMisses;
    extern const Event MarkCacheEvictedBytes;
    extern const Event MarkCacheEvictedMarks;
    extern const Event MarkCacheEvictedFiles;
}

namespace DB
{

/// Estimate of number of bytes in cache for marks.
struct MarksWeightFunction
{
    /// We spent additional bytes on key in hashmap, linked lists, shared pointers, etc ...
    static constexpr size_t MARK_CACHE_OVERHEAD = 128;

    size_t operator()(const MarksInCompressedFile & marks) const
    {
        return marks.approximateMemoryUsage() + MARK_CACHE_OVERHEAD;
    }
};

extern template class CacheBase<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>;
/** Cache of 'marks' for StorageMergeTree.
  * Marks is an index structure that addresses ranges in column file, corresponding to ranges of primary key.
  */
class MarkCache : public CacheBase<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>
{
private:
    using Base = CacheBase<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>;

public:
    MarkCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    /// Calculate key from path to file.
    static UInt128 hash(const String & path_to_file);

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::MarkCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::MarkCacheHits);

        return result.first;
    }

private:
    /// Called when the cache is full, and we evict some cells as per the cache policy.
    void onEviction(const EvictionDetails& details) override
    {
        ProfileEvents::increment(ProfileEvents::MarkCacheEvictedBytes, details.total_weight_loss);
        ProfileEvents::increment(ProfileEvents::MarkCacheEvictedMarks,
            /// Sum up the total number of marks across all evicted cache entries
            std::accumulate(details.evicted_values.begin(), details.evicted_values.end(), 0ULL,
            [](size_t const sum, const MappedPtr& ptr)
            {
                return sum + std::static_pointer_cast<MarksInCompressedFile>(ptr)->getNumberOfMarks();
            }));
        /// number of files evicted from the cache is same as number of evicted values as both are tied together as a cell in the cache
        ProfileEvents::increment(ProfileEvents::MarkCacheEvictedFiles, details.evicted_values.size());
    }
};

using MarkCachePtr = std::shared_ptr<MarkCache>;

}
