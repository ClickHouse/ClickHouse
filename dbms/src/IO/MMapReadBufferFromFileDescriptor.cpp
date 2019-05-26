#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <IO/MMapReadBufferFromFileDescriptor.h>


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


void MMapReadBufferFromFileDescriptor::init(int fd_, size_t offset, size_t length_)
{
    fd = fd_;
    length = length_;

    if (length)
    {
        void * buf = mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, offset);
        if (MAP_FAILED == buf)
            throwFromErrno("MMapReadBufferFromFileDescriptor: Cannot mmap " + formatReadableSizeWithBinarySuffix(length) + ".",
                ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        BufferBase::set(static_cast<char *>(buf), length, 0);
    }
}

void MMapReadBufferFromFileDescriptor::init(int fd_, size_t offset)
{
    fd = fd_;

    struct stat stat_res {};
    if (0 != fstat(fd, &stat_res))
        throwFromErrno("MMapReadBufferFromFileDescriptor: Cannot fstat.", ErrorCodes::CANNOT_STAT);

    off_t file_size = stat_res.st_size;

    if (file_size < 0)
        throw Exception("MMapReadBufferFromFileDescriptor: fstat returned negative file size", ErrorCodes::LOGICAL_ERROR);

    if (offset > static_cast<size_t>(file_size))
        throw Exception("MMapReadBufferFromFileDescriptor: requested offset is greater than file size", ErrorCodes::BAD_ARGUMENTS);

    init(fd, offset, file_size - offset);
}


MMapReadBufferFromFileDescriptor::MMapReadBufferFromFileDescriptor(int fd, size_t offset, size_t length)
    : MMapReadBufferFromFileDescriptor()
{
    init(fd, offset, length);
}


MMapReadBufferFromFileDescriptor::MMapReadBufferFromFileDescriptor(int fd, size_t offset)
    : MMapReadBufferFromFileDescriptor()
{
    init(fd, offset);
}


MMapReadBufferFromFileDescriptor::~MMapReadBufferFromFileDescriptor()
{
    if (length)
        finish();    /// Exceptions will lead to std::terminate and that's Ok.
}


void MMapReadBufferFromFileDescriptor::finish()
{
    if (0 != munmap(internalBuffer().begin(), length))
        throwFromErrno("MMapReadBufferFromFileDescriptor: Cannot munmap " + formatReadableSizeWithBinarySuffix(length) + ".",
            ErrorCodes::CANNOT_MUNMAP);

    length = 0;
}

}
