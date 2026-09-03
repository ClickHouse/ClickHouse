#pragma once

#include <Core/Types.h>
#include <Common/HashTable/Hash.h>
#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <IO/MMappedFile.h>


namespace ProfileEvents
{
    extern const Event MMappedFileCacheHits;
    extern const Event MMappedFileCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric MMapCacheCells;
}

namespace DB
{


/** Cache of opened and mmapped files for reading.
  * mmap/munmap is heavy operation and better to keep mapped file to subsequent use than to map/unmap every time.
  */
class MMappedFileCache : public CacheBase<UInt128, MMappedFile, UInt128TrivialHash>
{
private:
    using Base = CacheBase<UInt128, MMappedFile, UInt128TrivialHash>;

public:
    explicit MMappedFileCache(size_t max_size_in_cells)
        // Note, it is OK to use max_size_in_bytes=max_size_in_cells since default weight is 1
        : Base(CurrentMetrics::end(), CurrentMetrics::MMapCacheCells, /*max_size_in_bytes=*/ max_size_in_cells)
    {}

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file, size_t offset, ssize_t length = -1);

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

