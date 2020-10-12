#include <IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFile.h>
#if defined(OS_LINUX) || defined(__FreeBSD__)
#include <IO/ReadBufferAIO.h>
#endif
#include <IO/MMapReadBufferFromFile.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event CreatedReadBufferOrdinary;
    extern const Event CreatedReadBufferAIO;
    extern const Event CreatedReadBufferAIOFailed;
    extern const Event CreatedReadBufferMMap;
    extern const Event CreatedReadBufferMMapFailed;
}

namespace DB
{

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase(
    const std::string & filename_,
    size_t estimated_size, size_t aio_threshold, size_t mmap_threshold,
    size_t buffer_size_, int flags_, char * existing_memory_, size_t alignment)
{
#if defined(OS_LINUX) || defined(__FreeBSD__)
    if (aio_threshold && estimated_size >= aio_threshold)
    {
        /// Attempt to open a file with O_DIRECT
        try
        {
            auto res = std::make_unique<ReadBufferAIO>(filename_, buffer_size_, flags_, existing_memory_);
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferAIO);
            return res;
        }
        catch (const ErrnoException &)
        {
            /// Fallback to cached IO if O_DIRECT is not supported.
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferAIOFailed);
        }
    }
#else
    (void)aio_threshold;
    (void)estimated_size;
#endif

    if (!existing_memory_ && mmap_threshold && estimated_size >= mmap_threshold)
    {
        try
        {
            auto res = std::make_unique<MMapReadBufferFromFile>(filename_, 0);
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
    return std::make_unique<ReadBufferFromFile>(filename_, buffer_size_, flags_, existing_memory_, alignment);
}

}
