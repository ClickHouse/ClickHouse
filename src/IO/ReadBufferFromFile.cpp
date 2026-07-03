#include <fcntl.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Common/ProfileEvents.h>
#include <base/defines.h>
#include <cerrno>


namespace ProfileEvents
{
    extern const Event FileOpen;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CLOSE_FILE;
}


ReadBufferFromFile::ReadBufferFromFile(
    const std::string & file_name_,
    size_t buf_size,
    int flags,
    char * existing_memory,
    size_t alignment,
    std::optional<size_t> file_size_,
    ThrottlerPtr throttler_)
    : ReadBufferFromFileDescriptor(-1, buf_size, existing_memory, alignment, file_size_, throttler_)
    , file_name(file_name_)
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

#ifdef OS_DARWIN
    bool o_direct = (flags != -1) && (flags & O_DIRECT);
    if (o_direct)
        flags = flags & ~O_DIRECT;
#endif
    fd = ::open(file_name.c_str(), flags == -1 ? O_RDONLY | O_CLOEXEC : flags | O_CLOEXEC);

    if (-1 == fd)
        ErrnoException::throwFromPath(
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE, file_name, "Cannot open file {}", file_name);
#ifdef OS_DARWIN
    if (o_direct)
    {
        if (fcntl(fd, F_NOCACHE, 1) == -1)
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, file_name, "Cannot set F_NOCACHE on file {}", file_name);
    }
#endif
}


ReadBufferFromFile::ReadBufferFromFile(
    int & fd_,
    const std::string & original_file_name,
    size_t buf_size,
    char * existing_memory,
    size_t alignment,
    std::optional<size_t> file_size_,
    ThrottlerPtr throttler_)
    : ReadBufferFromFileDescriptor(fd_, buf_size, existing_memory, alignment, file_size_, throttler_)
    , file_name(original_file_name.empty() ? "(fd = " + toString(fd_) + ")" : original_file_name)
{
    fd_ = -1;
}


ReadBufferFromFile::~ReadBufferFromFile()
{
    if (fd < 0)
        return;

    [[maybe_unused]] int err = ::close(fd);
    chassert(!err || errno == EINTR);
}


void ReadBufferFromFile::close()
{
    if (fd < 0)
        return;

    if (0 != ::close(fd))
    {
        fd = -1;
        throw Exception(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file");
    }

    fd = -1;
    metric_increment.destroy();
}

}
