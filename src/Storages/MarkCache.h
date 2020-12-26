#pragma once

#include <memory>

#include <Common/ArrayCache.h>
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

/** Cache of 'marks' for StorageMergeTree.
  * Marks is an index structure that addresses ranges in column file, corresponding to ranges of primary key.
  */
class MarkCache : public ArrayCache<UInt128, MarksInCompressedFile>
{
private:
    using Base = ArrayCache<UInt128, MarksInCompressedFile>;

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

    HolderPtr get(const Key & key)
    {
        Base::HolderPtr res = Base::get(key);

        if (res)
            ProfileEvents::increment(ProfileEvents::MarkCacheHits);
        else
            ProfileEvents::increment(ProfileEvents::MarkCacheMisses);

        return res;
    }

    template <typename GetSizeFunc, typename InitializeFunc>
    HolderPtr getOrSet(const Key & key, GetSizeFunc && get_size, InitializeFunc && initialize)
    {
        bool was_calculated = false;
        auto result = Base::getOrSet(key, get_size, initialize, &was_calculated);

        if (was_calculated)
            ProfileEvents::increment(ProfileEvents::MarkCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::MarkCacheHits);

        return result;
    }
};

using MarkCachePtr = std::shared_ptr<MarkCache>;

}
