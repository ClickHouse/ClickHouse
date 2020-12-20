#include "CachedCompressedReadBuffer.h"

#include <IO/WriteHelpers.h>
#include <Compression/LZ4_decompress_faster.h>

#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


void CachedCompressedReadBuffer::initInput()
{
    if (!file_in)
    {
        file_in = file_in_creator();
        compressed_in = file_in.get();

        if (profile_callback)
            file_in->setProfileCallback(profile_callback, clock_type);
    }
}


bool CachedCompressedReadBuffer::nextImpl()
{

    /// Let's check for the presence of a decompressed block in the cache, grab the ownership of this block, if it exists.
    UInt128 key = cache->hash(path, file_pos);

    size_t size_decompressed = 0;
    size_t size_compressed_without_checksum = 0;
    size_t size_compressed = 0;
    size_t additional_bytes = 0;

    ///TODO: Fix interface probably payload can contain size then we can make cell
    /// and return it as payload
    owned_region = cache->getOrSet(
        key,
        [&]() -> size_t
        {
            initInput();
            file_in->seek(file_pos, SEEK_SET);
            size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum, false);

            if (size_compressed)
            {
                additional_bytes = codec->getAdditionalSizeAtTheEndOfBuffer();
                return size_decompressed + additional_bytes;
            }
            else
            {
                return 0;
            }
        },
        [&](void * ptr, UncompressedCacheCell & payload)
        {
            payload.compressed_size = size_compressed;
            
            if (payload.compressed_size)
            {
                payload.additional_bytes = additional_bytes;
                decompress(static_cast<char *>(ptr), size_decompressed, size_compressed_without_checksum);
            }
        },
        nullptr);

    auto owned_cell = owned_region->payload();
    auto size = owned_region->size();

    if (owned_region->size() == 0)
        return false;

    auto buffer_begin = static_cast<char*>(owned_region->ptr());
    working_buffer = Buffer(buffer_begin, buffer_begin + size - owned_cell.additional_bytes);

    file_pos += owned_cell.compressed_size;

    return true;
}

CachedCompressedReadBuffer::CachedCompressedReadBuffer(
    const std::string & path_, std::function<std::unique_ptr<ReadBufferFromFileBase>()> file_in_creator_, UncompressedCache * cache_, bool allow_different_codecs_)
    : ReadBuffer(nullptr, 0), file_in_creator(std::move(file_in_creator_)), cache(cache_), path(path_), file_pos(0)
{
    allow_different_codecs = allow_different_codecs_;
}

void CachedCompressedReadBuffer::seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
{
    if (owned_region &&
        offset_in_compressed_file == file_pos - owned_region->payload().compressed_size &&
        offset_in_decompressed_block <= working_buffer.size())
    {
        bytes += offset();
        pos = working_buffer.begin() + offset_in_decompressed_block;
        bytes -= offset();
    }
    else
    {
        file_pos = offset_in_compressed_file;

        bytes += offset();
        nextImpl();

        if (offset_in_decompressed_block > working_buffer.size())
            throw Exception("Seek position is beyond the decompressed block"
                " (pos: " + toString(offset_in_decompressed_block) + ", block size: " + toString(working_buffer.size()) + ")",
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

        pos = working_buffer.begin() + offset_in_decompressed_block;
        bytes -= offset();
    }
}

}
