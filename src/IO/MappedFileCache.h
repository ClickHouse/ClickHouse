#pragma once

#include <Common/LRUCache.h>
#include <Common/SipHash.h>
#include <Common/UInt128.h>
#include <Common/ProfileEvents.h>
#include <IO/MappedFile.h>


namespace ProfileEvents
{
    extern const Event MappedFileCacheHits;
    extern const Event MappedFileCacheMisses;
}

namespace DB
{


/** Cache of opened and mmapped files for reading.
  * mmap/munmap is heavy operation and better to keep mapped file to subsequent use than to map/unmap every time.
  */
class MappedFileCache : public LRUCache<UInt128, MappedFile, UInt128TrivialHash>
{
private:
    using Base = LRUCache<UInt128, MappedFile, UInt128TrivialHash>;

public:
    MappedFileCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes) {}

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file, size_t offset, ssize_t length = -1)
    {
        UInt128 key;

        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);
        hash.update(offset);
        hash.update(length);

        hash.get128(key.low, key.high);

        return key;
    }

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::MappedFileCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::MappedFileCacheHits);

        return result.first;
    }
};

using MappedFileCachePtr = std::shared_ptr<MappedFileCache>;

}

