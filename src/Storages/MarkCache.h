#pragma once

#include <memory>

#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Interpreters/AggregationCommon.h>
#include <Formats/MarkInCompressedFile.h>


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
    /// We spent additional bytes on key in hashmap, linked lists, shared pointers, etc ...
    static constexpr size_t MARK_CACHE_OVERHEAD = 128;

    size_t operator()(const MarksInCompressedFile & marks) const
    {
        return marks.size() * sizeof(MarkInCompressedFile) + MARK_CACHE_OVERHEAD;
    }
};


/** Cache of 'marks' for StorageMergeTree.
  * Marks is an index structure that addresses ranges in column file, corresponding to ranges of primary key.
  */
class MarkCache : public CacheBase<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>
{
private:
    using Base = CacheBase<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>;

public:
    explicit MarkCache(size_t max_size_in_bytes, String mark_cache_policy = "")
        : Base(mark_cache_policy, max_size_in_bytes) {}

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file)
    {
        UInt128 key;

        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);
        hash.get128(key);

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
