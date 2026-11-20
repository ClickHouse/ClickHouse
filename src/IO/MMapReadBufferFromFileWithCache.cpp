#include <IO/MMapReadBufferFromFileWithCache.h>
#include <base/getPageSize.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


void MMapReadBufferFromFileWithCache::init()
{
    size_t length = mapped->getLength();
    BufferBase::set(mapped->getData(), length, 0);

    size_t page_size = static_cast<size_t>(::getPageSize());
    ReadBuffer::padded = (length % page_size) > 0 && (length % page_size) <= (page_size - (PADDING_FOR_SIMD - 1));
    ReadBufferFromFileBase::file_size = length;
}


MMapReadBufferFromFileWithCache::MMapReadBufferFromFileWithCache(
    MMappedFileCache & cache, const std::string & file_name, size_t offset, size_t length)
{
    mapped = cache.getOrSet(MMappedFileCache::hash(file_name, offset, length), [&]
    {
        return std::make_shared<MMappedFile>(file_name, offset, length);
    });

    init();
}

MMapReadBufferFromFileWithCache::MMapReadBufferFromFileWithCache(
    MMappedFileCache & cache, const std::string & file_name, size_t offset)
{
    mapped = cache.getOrSet(MMappedFileCache::hash(file_name, offset, -1), [&]
    {
        return std::make_shared<MMappedFile>(file_name, offset);
    });

    init();
}


std::string MMapReadBufferFromFileWithCache::getFileName() const
{
    return mapped->getFileName();
}

off_t MMapReadBufferFromFileWithCache::getPosition()
{
    return count();
}

off_t MMapReadBufferFromFileWithCache::seek(off_t offset, int whence)
{
    off_t new_pos;
    if (whence == SEEK_SET)
        new_pos = offset;
    else if (whence == SEEK_CUR)
        new_pos = count() + offset;
    else
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "MMapReadBufferFromFileWithCache::seek expects SEEK_SET or SEEK_CUR as whence");

    working_buffer = internal_buffer;
    if (new_pos < 0 || new_pos > off_t(working_buffer.size()))
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
            "Cannot seek through file {} because seek position ({}) is out of bounds [0, {}]",
            getFileName(), new_pos, working_buffer.size());

    position() = working_buffer.begin() + new_pos;
    return new_pos;
}

}
