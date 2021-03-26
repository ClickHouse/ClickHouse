#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <fmt/format.h>

#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <common/getPageSize.h>
#include <IO/MappedFileDescriptor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_MUNMAP;
    extern const int CANNOT_STAT;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


static size_t getFileSize(int fd)
{
    struct stat stat_res {};
    if (0 != fstat(fd, &stat_res))
        throwFromErrno("MappedFileDescriptor: Cannot fstat.", ErrorCodes::CANNOT_STAT);

    off_t file_size = stat_res.st_size;

    if (file_size < 0)
        throw Exception("MappedFileDescriptor: fstat returned negative file size", ErrorCodes::LOGICAL_ERROR);

    return file_size;
}


MappedFileDescriptor::MappedFileDescriptor(int fd_, size_t offset_, size_t length_)
{
    set(fd_, offset_, length_);
}

MappedFileDescriptor::MappedFileDescriptor(int fd_, size_t offset_)
    : fd(fd_), offset(offset_)
{
    set(fd_, offset_);
}

void MappedFileDescriptor::set(int fd_, size_t offset_, size_t length_)
{
    finish();

    fd = fd_;
    offset = offset_;
    length = length_;

    if (length)
    {
        void * buf = mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, offset);
        if (MAP_FAILED == buf)
            throwFromErrno(fmt::format("MappedFileDescriptor: Cannot mmap {}.", ReadableSize(length)),
                ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        data = static_cast<char *>(buf);
    }
}

void MappedFileDescriptor::set(int fd_, size_t offset_)
{
    size_t file_size = getFileSize(fd_);

    if (offset > static_cast<size_t>(file_size))
        throw Exception("MappedFileDescriptor: requested offset is greater than file size", ErrorCodes::BAD_ARGUMENTS);

    set(fd_, offset_, file_size - offset);
}

void MappedFileDescriptor::finish()
{
    if (!length)
        return;

    if (0 != munmap(data, length))
        throwFromErrno(fmt::format("MappedFileDescriptor: Cannot munmap {}.", ReadableSize(length)),
            ErrorCodes::CANNOT_MUNMAP);

    length = 0;
}

MappedFileDescriptor::~MappedFileDescriptor()
{
    finish(); /// Exceptions will lead to std::terminate and that's Ok.
}

}


