#include <IO/createWriteBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#if defined(OS_LINUX) || defined(__FreeBSD__)
#include <IO/WriteBufferAIO.h>
#endif
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event CreatedWriteBufferOrdinary;
    extern const Event CreatedWriteBufferAIO;
    extern const Event CreatedWriteBufferAIOFailed;
}

namespace DB
{

std::unique_ptr<WriteBufferFromFileBase> createWriteBufferFromFileBase(const std::string & filename_, size_t estimated_size,
        size_t aio_threshold, size_t buffer_size_, int flags_, mode_t mode, char * existing_memory_,
        size_t alignment)
{
#if defined(OS_LINUX) || defined(__FreeBSD__)
    if (aio_threshold && estimated_size >= aio_threshold)
    {
        /// Attempt to open a file with O_DIRECT
        try
        {
            auto res = std::make_unique<WriteBufferAIO>(filename_, buffer_size_, flags_, mode, existing_memory_);
            ProfileEvents::increment(ProfileEvents::CreatedWriteBufferAIO);
            return res;
        }
        catch (const ErrnoException &)
        {
            /// Fallback to cached IO if O_DIRECT is not supported.
            ProfileEvents::increment(ProfileEvents::CreatedWriteBufferAIOFailed);
        }
    }
#else
    (void)aio_threshold;
    (void)estimated_size;
#endif

    ProfileEvents::increment(ProfileEvents::CreatedWriteBufferOrdinary);
    return std::make_unique<WriteBufferFromFile>(filename_, buffer_size_, flags_, mode, existing_memory_, alignment);
}

}
