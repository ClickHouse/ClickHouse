#include <IO/createWriteBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#if defined(__linux__) || defined(__FreeBSD__)
#include <IO/WriteBufferAIO.h>
#endif
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event CreatedWriteBufferOrdinary;
    extern const Event CreatedWriteBufferAIO;
}

namespace DB
{

#if !defined(__linux__)
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
#endif

std::unique_ptr<WriteBufferFromFileBase> createWriteBufferFromFileBase(const std::string & filename_, size_t estimated_size,
        size_t aio_threshold, size_t buffer_size_, int flags_, mode_t mode, char * existing_memory_,
        size_t alignment)
{
    if ((aio_threshold == 0) || (estimated_size < aio_threshold))
    {
        ProfileEvents::increment(ProfileEvents::CreatedWriteBufferOrdinary);
        return std::make_unique<WriteBufferFromFile>(filename_, buffer_size_, flags_, mode, existing_memory_, alignment);
    }
    else
    {
#if defined(__linux__) || defined(__FreeBSD__)
        ProfileEvents::increment(ProfileEvents::CreatedWriteBufferAIO);
        return std::make_unique<WriteBufferAIO>(filename_, buffer_size_, flags_, mode, existing_memory_);
#else
        throw Exception("AIO is not implemented yet on non-Linux OS", ErrorCodes::NOT_IMPLEMENTED);
#endif
    }
}

}
