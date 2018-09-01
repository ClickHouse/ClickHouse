#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

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
    : WriteBufferFromFileDescriptor(-1, buf_size, existing_memory, alignment), file_name(file_name_)
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

#ifdef __APPLE__
    bool o_direct = (flags != -1) && (flags & O_DIRECT);
    if (o_direct)
        flags = flags & ~O_DIRECT;
#endif

    fd = open(file_name.c_str(), flags == -1 ? O_WRONLY | O_TRUNC | O_CREAT : flags, mode);

    if (-1 == fd)
        throwFromErrno("Cannot open file " + file_name, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);

#ifdef __APPLE__
    if (o_direct)
    {
        if (fcntl(fd, F_NOCACHE, 1) == -1)
            throwFromErrno("Cannot set F_NOCACHE on file " + file_name, ErrorCodes::CANNOT_OPEN_FILE);
    }
#endif
}


/// Use pre-opened file descriptor.
WriteBufferFromFile::WriteBufferFromFile(
    int fd,
    const std::string & original_file_name,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    :
    WriteBufferFromFileDescriptor(fd, buf_size, existing_memory, alignment),
    file_name(original_file_name.empty() ? "(fd = " + toString(fd) + ")" : original_file_name)
{
}


WriteBufferFromFile::~WriteBufferFromFile()
{
    if (fd < 0)
        return;

    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    ::close(fd);
}


/// Close file before destruction of object.
void WriteBufferFromFile::close()
{
    next();

    if (0 != ::close(fd))
        throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

    fd = -1;
    metric_increment.destroy();
}

}
