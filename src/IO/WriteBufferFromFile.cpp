#include <sys/stat.h>
#include <fcntl.h>
#include <cerrno>

#include <Common/ProfileEvents.h>
#include <base/defines.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>


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


WriteBufferFromFile::WriteBufferFromFile(
    const std::string & file_name_,
    size_t buf_size,
    int flags,
    ThrottlerPtr throttler_,
    mode_t mode,
    char * existing_memory,
    size_t alignment)
    : WriteBufferFromFileDescriptor(-1, buf_size, existing_memory, throttler_, alignment, file_name_)
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

#ifdef OS_DARWIN
    bool o_direct = (flags != -1) && (flags & O_DIRECT);
    if (o_direct)
        flags = flags & ~O_DIRECT;
#endif

    fd = ::open(file_name.c_str(), flags == -1 ? O_WRONLY | O_TRUNC | O_CREAT | O_CLOEXEC : flags | O_CLOEXEC, mode);

    if (-1 == fd)
        throwFromErrnoWithPath("Cannot open file " + file_name, file_name,
                               errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);

#ifdef OS_DARWIN
    if (o_direct)
    {
        if (fcntl(fd, F_NOCACHE, 1) == -1)
            throwFromErrnoWithPath("Cannot set F_NOCACHE on file " + file_name, file_name, ErrorCodes::CANNOT_OPEN_FILE);
    }
#endif
}


/// Use pre-opened file descriptor.
WriteBufferFromFile::WriteBufferFromFile(
    int & fd_,
    const std::string & original_file_name,
    size_t buf_size,
    ThrottlerPtr throttler_,
    char * existing_memory,
    size_t alignment)
    : WriteBufferFromFileDescriptor(fd_, buf_size, existing_memory, throttler_, alignment, original_file_name)
{
    fd_ = -1;
}

WriteBufferFromFile::~WriteBufferFromFile()
{
    if (fd < 0)
        return;

    finalize();
    int err = ::close(fd);
    /// Everything except for EBADF should be ignored in dtor, since all of
    /// others (EINTR/EIO/ENOSPC/EDQUOT) could be possible during writing to
    /// fd, and then write already failed and the error had been reported to
    /// the user/caller.
    ///
    /// Note, that for close() on Linux, EINTR should *not* be retried.
    chassert(!(err && errno == EBADF));
}

void WriteBufferFromFile::finalizeImpl()
{
    if (fd < 0)
        return;

    next();
}


/// Close file before destruction of object.
void WriteBufferFromFile::close()
{
    if (fd < 0)
        return;

    finalize();

    if (0 != ::close(fd))
        throw Exception(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file");

    fd = -1;
    metric_increment.destroy();
}

}
