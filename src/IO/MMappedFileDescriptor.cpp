#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <base/getPageSize.h>
#include <IO/MMappedFileDescriptor.h>


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
        throw ErrnoException(ErrorCodes::CANNOT_STAT, "MMappedFileDescriptor: Cannot fstat");

    off_t file_size = stat_res.st_size;

    if (file_size < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MMappedFileDescriptor: fstat returned negative file size");

    return file_size;
}


MMappedFileDescriptor::MMappedFileDescriptor(int fd_, size_t offset_, size_t length_)
{
    set(fd_, offset_, length_);
}

MMappedFileDescriptor::MMappedFileDescriptor(int fd_, size_t offset_)
    : fd(fd_), offset(offset_)
{
    set(fd_, offset_);
}

void MMappedFileDescriptor::set(int fd_, size_t offset_, size_t length_)
{
    finish();

    fd = fd_;
    offset = offset_;
    length = length_;

    if (!length)
        return;

    void * buf = mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, offset);
    if (MAP_FAILED == buf)
        throw ErrnoException(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "MMappedFileDescriptor: Cannot mmap {}", ReadableSize(length));

    data = static_cast<char *>(buf);

    files_metric_increment.changeTo(1);
    bytes_metric_increment.changeTo(length);
}

void MMappedFileDescriptor::set(int fd_, size_t offset_)
{
    size_t file_size = getFileSize(fd_);

    if (offset > file_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MMappedFileDescriptor: requested offset is greater than file size");

    set(fd_, offset_, file_size - offset);
}

void MMappedFileDescriptor::finish()
{
    if (!length)
        return;

    if (0 != munmap(data, length))
        throw ErrnoException(ErrorCodes::CANNOT_MUNMAP, "MMappedFileDescriptor: Cannot munmap {}", ReadableSize(length));

    length = 0;

    files_metric_increment.changeTo(0);
    bytes_metric_increment.changeTo(0);
}

MMappedFileDescriptor::~MMappedFileDescriptor()
{
    finish(); /// Exceptions will lead to std::terminate and that's Ok.
}

}
