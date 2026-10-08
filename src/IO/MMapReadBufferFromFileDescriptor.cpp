#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>
#include <base/getPageSize.h>
#include <IO/WriteHelpers.h>
#include <IO/MMapReadBufferFromFileDescriptor.h>
#include <IO/preadNoWait.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}

namespace
{

/// Best effort: a failed hint must not fail a read.
void adviseWillNeedOnce([[maybe_unused]] int fd, [[maybe_unused]] size_t offset, [[maybe_unused]] size_t n)
{
#if defined(POSIX_FADV_WILLNEED)
    [[maybe_unused]] int res = ::posix_fadvise(fd, static_cast<off_t>(offset), static_cast<off_t>(n), POSIX_FADV_WILLNEED);
#endif
}

/// One hint makes Linux read at most max(read_ahead_kb, max_sectors_kb) of the range, so a longer range is
/// advised in pieces that fit the default limit.
void adviseWillNeedInPieces([[maybe_unused]] int fd, [[maybe_unused]] size_t offset, [[maybe_unused]] size_t size)
{
#if defined(POSIX_FADV_WILLNEED)
    constexpr size_t piece_size = 128 * 1024;
    for (size_t pos = 0; pos < size; pos += piece_size)
        [[maybe_unused]] int res = ::posix_fadvise(
            fd, static_cast<off_t>(offset + pos), static_cast<off_t>(std::min(piece_size, size - pos)), POSIX_FADV_WILLNEED);
#endif
}

}


void MMapReadBufferFromFileDescriptor::init()
{
    size_t length = mapped.getLength();
    BufferBase::set(mapped.getData(), length, 0);

    size_t page_size = static_cast<size_t>(::getPageSize());
    ReadBuffer::padded = (length % page_size) > 0 && (length % page_size) <= (page_size - (PADDING_FOR_SIMD - 1));
}


MMapReadBufferFromFileDescriptor::MMapReadBufferFromFileDescriptor(int fd, size_t offset, size_t length)
    : mapped(fd, offset, length)
{
    init();
}


MMapReadBufferFromFileDescriptor::MMapReadBufferFromFileDescriptor(int fd, size_t offset)
    : mapped(fd, offset)
{
    init();
}


void MMapReadBufferFromFileDescriptor::finish()
{
    mapped.finish();
}


std::string MMapReadBufferFromFileDescriptor::getFileName() const
{
    return "(fd = " + toString(mapped.getFD()) + ")";
}

int MMapReadBufferFromFileDescriptor::getFD() const
{
    return mapped.getFD();
}

off_t MMapReadBufferFromFileDescriptor::getPosition()
{
    return count();
}

off_t MMapReadBufferFromFileDescriptor::seek(off_t offset, int whence)
{
    off_t new_pos = 0;
    if (whence == SEEK_SET)
        new_pos = offset;
    else if (whence == SEEK_CUR)
        new_pos = count() + offset;
    else
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "MMapReadBufferFromFileDescriptor::seek expects SEEK_SET or SEEK_CUR as whence");

    working_buffer = internal_buffer;
    if (new_pos < 0 || new_pos > off_t(working_buffer.size()))
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
            "Cannot seek through file {} because seek position ({}) is out of bounds [0, {}]",
            getFileName(), new_pos, working_buffer.size());

    position() = working_buffer.begin() + new_pos;
    return new_pos;
}

std::optional<size_t> MMapReadBufferFromFileDescriptor::tryGetFileSize()
{
    return getSizeFromFileDescriptor(getFD(), getFileName());
}

size_t MMapReadBufferFromFileDescriptor::readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t)> &) const
{
    if (offset >= mapped.getLength())
        return 0;

    n = std::min(n, mapped.getLength() - offset);

    /// A page fault on a page that is not in the page cache reads the whole read-ahead window around it.
    char probe = 0;
    if (n > 0 && preadNoWait(mapped.getFD(), &probe, 1, mapped.getOffset() + offset) != 1)
        adviseWillNeedInPieces(mapped.getFD(), mapped.getOffset() + offset, n);

    memcpy(to, mapped.getData() + offset, n);
    return n;
}

VectorWithMemoryTracking<SeekableReadBuffer::CachedRegion> MMapReadBufferFromFileDescriptor::readBigAtRetainCells(size_t n, size_t offset) const
{
    if (n == 0 || offset >= mapped.getLength())
        return {};

    n = std::min(n, mapped.getLength() - offset);
    adviseWillNeedOnce(mapped.getFD(), mapped.getOffset() + offset, n);

    VectorWithMemoryTracking<CachedRegion> regions;
    regions.push_back(CachedRegion{.handle = nullptr, .data = mapped.getData() + offset, .size = n, .file_offset = offset});
    return regions;
}

void MMapReadBufferFromFileDescriptor::adviseWillNeed(size_t offset, size_t n) const
{
    if (offset >= mapped.getLength())
        return;
    adviseWillNeedInPieces(mapped.getFD(), mapped.getOffset() + offset, std::min(n, mapped.getLength() - offset));
}

}
