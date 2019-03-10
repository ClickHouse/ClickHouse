#include <IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFile.h>
#if defined(__linux__) || defined(__FreeBSD__)
#include <IO/ReadBufferAIO.h>
#endif
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event CreatedReadBufferOrdinary;
    extern const Event CreatedReadBufferAIO;
}

namespace DB
{
#if !defined(__linux__)
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
#endif

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase(const std::string & filename_, size_t estimated_size,
        size_t aio_threshold, size_t buffer_size_, int flags_, char * existing_memory_, size_t alignment)
{
    if ((aio_threshold == 0) || (estimated_size < aio_threshold))
    {
        ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);
        return std::make_unique<ReadBufferFromFile>(filename_, buffer_size_, flags_, existing_memory_, alignment);
    }
    else
    {
#if defined(__linux__) || defined(__FreeBSD__)
        ProfileEvents::increment(ProfileEvents::CreatedReadBufferAIO);
        return std::make_unique<ReadBufferAIO>(filename_, buffer_size_, flags_, existing_memory_);
#else
        throw Exception("AIO is implemented only on Linux and FreeBSD", ErrorCodes::NOT_IMPLEMENTED);
#endif
    }
}

}
