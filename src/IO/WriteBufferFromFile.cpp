#include <sys/stat.h>
#include <fcntl.h>
#include <cerrno>

#include <Common/ProfileEvents.h>

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
    mode_t mode,
    char * existing_memory,
    size_t alignment)
    : WriteBufferFromFileDescriptor(-1, buf_size, existing_memory, alignment, file_name_)
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
    char * existing_memory,
    size_t alignment)
    : WriteBufferFromFileDescriptor(fd_, buf_size, existing_memory, alignment, original_file_name)
{
    fd_ = -1;
}

WriteBufferFromFile::~WriteBufferFromFile()
{
    ::close(fd);
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

    next();

    if (0 != ::close(fd))
        throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

    fd = -1;
    metric_increment.destroy();
}

}
