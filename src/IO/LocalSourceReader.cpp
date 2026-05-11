#include <IO/LocalSourceReader.h>
#include <IO/ReadSettings.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <base/errnoToString.h>

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

namespace ProfileEvents
{
    extern const Event FileOpen;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CLOSE_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}

size_t LocalSourceReader::read(
    const StoredObject & object,
    size_t offset, size_t size,
    char * buffer)
{
    LOG_TRACE(log, "read: file={}, offset={}, size={}", object.remote_path, offset, size);

    int fd = ::open(object.remote_path.c_str(), O_RDONLY);
    if (fd < 0)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
            "Cannot open file {}: {}", object.remote_path, errnoToString());

    ProfileEvents::increment(ProfileEvents::FileOpen);

    size_t total_read = 0;
    while (total_read < size)
    {
        ssize_t res = ::pread(fd, buffer + total_read, size - total_read, offset + total_read);
        if (res < 0)
        {
            int err = errno;
            if (0 != ::close(fd))
                LOG_WARNING(log, "Cannot close file {}: {}", object.remote_path, errnoToString());
            throw Exception(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
                "Cannot pread from {}: {}", object.remote_path, errnoToString(err));
        }
        if (res == 0)
            break;
        total_read += res;
    }

    if (0 != ::close(fd))
        throw Exception(ErrorCodes::CANNOT_CLOSE_FILE,
            "Cannot close file {}: {}", object.remote_path, errnoToString());

    LOG_TRACE(log, "read: file={}, got {} bytes", object.remote_path, total_read);
    return total_read;
}

std::unique_ptr<ReadBufferFromFileBase> LocalSourceReader::open(const StoredObject & object)
{
    LOG_TRACE(log, "open: file={}", object.remote_path);
    return createReadBufferFromFileBase(object.remote_path, ReadSettings{});
}

}
