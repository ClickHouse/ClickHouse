#pragma once

#include <Core/Types.h>
#include <Common/HashTable/Hash.h>
#include <Common/LRUCache.h>
#include <Common/SipHash.h>
#include <Common/ProfileEvents.h>
#include <IO/MMappedFile.h>


namespace ProfileEvents
{
    extern const Event MMappedFileCacheHits;
    extern const Event MMappedFileCacheMisses;
}

namespace DB
{


/** Cache of opened and mmapped files for reading.
  * mmap/munmap is heavy operation and better to keep mapped file to subsequent use than to map/unmap every time.
  */
class MMappedFileCache : public LRUCache<UInt128, MMappedFile, UInt128TrivialHash>
{
private:
    using Base = LRUCache<UInt128, MMappedFile, UInt128TrivialHash>;

public:
    MMappedFileCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes) {}

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file, size_t offset, ssize_t length = -1)
    {
        UInt128 key;

        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);
        hash.update(offset);
        hash.update(length);

        hash.get128(key);
        return key;
    }

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::MMappedFileCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::MMappedFileCacheHits);

        return result.first;
    }
};

using MMappedFileCachePtr = std::shared_ptr<MMappedFileCache>;

}

