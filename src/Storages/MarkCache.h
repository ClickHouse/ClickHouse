#pragma once

#include <memory>

#include <Common/LRUCache.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Interpreters/AggregationCommon.h>
#include <DataStreams/MarkInCompressedFile.h>


namespace ProfileEvents
{
    extern const Event MarkCacheHits;
    extern const Event MarkCacheMisses;
}

namespace DB
{

/// Estimate of number of bytes in cache for marks.
struct MarksWeightFunction
{
    size_t operator()(const MarksInCompressedFile & marks) const
    {
        /// NOTE Could add extra 100 bytes for overhead of std::vector, cache structures and allocator.
        return marks.size() * sizeof(MarkInCompressedFile);
    }
};


/** Cache of 'marks' for StorageMergeTree.
  * Marks is an index structure that addresses ranges in column file, corresponding to ranges of primary key.
  */
class MarkCache : public LRUCache<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>
{
private:
    using Base = LRUCache<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>;

public:
    MarkCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes) {}

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file)
    {
        UInt128 key;

        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);
        hash.get128(key.low, key.high);

        return key;
    }

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
};

using MarkCachePtr = std::shared_ptr<MarkCache>;

}
