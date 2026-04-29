#include <IO/LocalSourceReader.h>
#include <Common/Exception.h>
#include <base/errnoToString.h>

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}

size_t LocalSourceReader::read(
    const StoredObject & object,
    size_t offset, size_t size,
    char * buffer)
{
    int fd = ::open(object.remote_path.c_str(), O_RDONLY);
    if (fd < 0)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
            "Cannot open file {}: {}", object.remote_path, errnoToString());

    size_t total_read = 0;
    while (total_read < size)
    {
        ssize_t res = ::pread(fd, buffer + total_read, size - total_read, offset + total_read);
        if (res < 0)
        {
            int err = errno;
            ::close(fd);
            throw Exception(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
                "Cannot pread from {}: {}", object.remote_path, errnoToString(err));
        }
        if (res == 0)
            break;
        total_read += res;
    }

    ::close(fd);
    return total_read;
}

}
