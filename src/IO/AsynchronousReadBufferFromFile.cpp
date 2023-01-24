#include <fcntl.h>

#include <IO/AsynchronousReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Common/ProfileEvents.h>
#include <errno.h>


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


AsynchronousReadBufferFromFile::AsynchronousReadBufferFromFile(
    AsynchronousReaderPtr reader_,
    Int32 priority_,
    const std::string & file_name_,
    size_t buf_size,
    int flags,
    char * existing_memory,
    size_t alignment,
    std::optional<size_t> file_size_)
    : AsynchronousReadBufferFromFileDescriptor(std::move(reader_), priority_, -1, buf_size, existing_memory, alignment, file_size_)
    , file_name(file_name_)
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

#ifdef __APPLE__
    bool o_direct = (flags != -1) && (flags & O_DIRECT);
    if (o_direct)
        flags = flags & ~O_DIRECT;
#endif
    fd = ::open(file_name.c_str(), flags == -1 ? O_RDONLY | O_CLOEXEC : flags | O_CLOEXEC);

    if (-1 == fd)
        throwFromErrnoWithPath("Cannot open file " + file_name, file_name,
                               errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
#ifdef __APPLE__
    if (o_direct)
    {
        if (fcntl(fd, F_NOCACHE, 1) == -1)
            throwFromErrnoWithPath("Cannot set F_NOCACHE on file " + file_name, file_name, ErrorCodes::CANNOT_OPEN_FILE);
    }
#endif
}


AsynchronousReadBufferFromFile::AsynchronousReadBufferFromFile(
    AsynchronousReaderPtr reader_,
    Int32 priority_,
    int & fd_,
    const std::string & original_file_name,
    size_t buf_size,
    char * existing_memory,
    size_t alignment,
    std::optional<size_t> file_size_)
    : AsynchronousReadBufferFromFileDescriptor(std::move(reader_), priority_, fd_, buf_size, existing_memory, alignment, file_size_)
    , file_name(original_file_name.empty() ? "(fd = " + toString(fd_) + ")" : original_file_name)
{
    fd_ = -1;
}


AsynchronousReadBufferFromFile::~AsynchronousReadBufferFromFile()
{
    /// Must wait for events in flight before closing the file.
    finalize();

    if (fd < 0)
        return;

    ::close(fd);
}


void AsynchronousReadBufferFromFile::close()
{
    if (fd < 0)
        return;

    if (0 != ::close(fd))
        throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

    fd = -1;
}


AsynchronousReadBufferFromFileWithDescriptorsCache::~AsynchronousReadBufferFromFileWithDescriptorsCache()
{
    /// Must wait for events in flight before potentially closing the file by destroying OpenedFilePtr.
    finalize();
}


}

