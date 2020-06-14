#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
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
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


void MMapReadBufferFromFileDescriptor::init(int fd_, size_t offset, size_t length_)
{
    fd = fd_;
    length = length_;

    if (length)
    {
        void * buf = mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, offset);
        if (MAP_FAILED == buf)
            throwFromErrno(fmt::format("MMapReadBufferFromFileDescriptor: Cannot mmap {}.", ReadableSize(length)),
                ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        BufferBase::set(static_cast<char *>(buf), length, 0);
        ReadBuffer::padded = (length % 4096) > 0 && (length % 4096) <= (4096 - 15); /// TODO determine page size
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


MMapReadBufferFromFileDescriptor::MMapReadBufferFromFileDescriptor(int fd_, size_t offset_, size_t length_)
{
    init(fd_, offset_, length_);
}


MMapReadBufferFromFileDescriptor::MMapReadBufferFromFileDescriptor(int fd_, size_t offset_)
{
    init(fd_, offset_);
}


MMapReadBufferFromFileDescriptor::~MMapReadBufferFromFileDescriptor()
{
    if (length)
        finish();    /// Exceptions will lead to std::terminate and that's Ok.
}


void MMapReadBufferFromFileDescriptor::finish()
{
    if (0 != munmap(internalBuffer().begin(), length))
        throwFromErrno(fmt::format("MMapReadBufferFromFileDescriptor: Cannot munmap {}.", ReadableSize(length)),
            ErrorCodes::CANNOT_MUNMAP);

    length = 0;
}

std::string MMapReadBufferFromFileDescriptor::getFileName() const
{
    return "(fd = " + toString(fd) + ")";
}

int MMapReadBufferFromFileDescriptor::getFD() const
{
    return fd;
}

off_t MMapReadBufferFromFileDescriptor::getPosition()
{
    return count();
}

off_t MMapReadBufferFromFileDescriptor::seek(off_t offset, int whence)
{
    off_t new_pos;
    if (whence == SEEK_SET)
        new_pos = offset;
    else if (whence == SEEK_CUR)
        new_pos = count() + offset;
    else
        throw Exception("MMapReadBufferFromFileDescriptor::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    working_buffer = internal_buffer;
    if (new_pos < 0 || new_pos > off_t(working_buffer.size()))
        throw Exception("Cannot seek through file " + getFileName()
            + " because seek position (" + toString(new_pos) + ") is out of bounds [0, " + toString(working_buffer.size()) + "]",
            ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    position() = working_buffer.begin() + new_pos;
    return new_pos;
}

}
