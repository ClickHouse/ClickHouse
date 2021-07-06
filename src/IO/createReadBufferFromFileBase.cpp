#include <IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/MMapReadBufferFromFileWithCache.h>
#include <Common/ProfileEvents.h>

#include <fcntl.h>


namespace ProfileEvents
{
    extern const Event CreatedReadBufferOrdinary;
    extern const Event CreatedReadBufferDirectIO;
    extern const Event CreatedReadBufferDirectIOFailed;
    extern const Event CreatedReadBufferMMap;
    extern const Event CreatedReadBufferMMapFailed;
}

namespace DB
{

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase(
    const std::string & filename,
    size_t estimated_size, size_t direct_io_threshold, size_t mmap_threshold, MMappedFileCache * mmap_cache,
    size_t buffer_size, int flags, char * existing_memory, size_t alignment)
{
    if (!existing_memory && mmap_threshold && mmap_cache && estimated_size >= mmap_threshold)
    {
        try
        {
            auto res = std::make_unique<MMapReadBufferFromFileWithCache>(*mmap_cache, filename, 0);
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferMMap);
            return res;
        }
        catch (const ErrnoException &)
        {
            /// Fallback if mmap is not supported (example: pipe).
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferMMapFailed);
        }
    }

    ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);
    auto res = std::make_unique<ReadBufferFromFile>(filename, buffer_size, flags, existing_memory, alignment);

    if (direct_io_threshold && estimated_size >= direct_io_threshold)
    {
#if defined(OS_LINUX)
        /** We don't use O_DIRECT because it is tricky and previous implementation has a bug.
          * Instead, we advise the OS that the data should not be cached.
          * This is not exactly the same for two reasons:
          * - extra copying from page cache to userspace is not eliminated;
          * - if data is already in cache, it is purged.
          *
          * NOTE: Better to rewrite it with userspace page cache.
          */

        if (0 != posix_fadvise(res->getFD(), 0, 0, POSIX_FADV_DONTNEED))
            LOG_WARNING(&Poco::Logger::get("createReadBufferFromFileBase"),
                "Cannot request 'posix_fadvise' with POSIX_FADV_DONTNEED for file {}", filename);
    }
#else
    (void)direct_io_threshold;
#endif

    return res;
}

}
