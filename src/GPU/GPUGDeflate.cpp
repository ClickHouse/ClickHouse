#include <GPU/GPUGDeflate.h>

#if USE_GPU

#include <Common/Exception.h>
#include <base/types.h>

#include <algorithm>
#include <cstring>

namespace DB::ErrorCodes
{
    extern const int CANNOT_DECOMPRESS;
}

namespace DB::GPU
{

namespace
{

constexpr size_t chunk_alignment = 4;

size_t alignUp(size_t bytes)
{
    return (bytes + chunk_alignment - 1) / chunk_alignment * chunk_alignment;
}

size_t numChunks(size_t uncompressed_bytes)
{
    return (uncompressed_bytes + gdeflate_chunk_bytes - 1) / gdeflate_chunk_bytes;
}

UInt32 readUInt32(const char * at)
{
    UInt32 value;
    memcpy(&value, at, sizeof(value));
    return value;
}

}

void forEachGDeflateChunk(std::string_view block, size_t uncompressed_bytes, const std::function<void(const GDeflateChunk &)> & on_chunk)
{
    if (block.size() < sizeof(UInt32))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "A GDeflate block of {} bytes", block.size());

    const size_t num_chunks = readUInt32(block.data());
    const size_t header_bytes = sizeof(UInt32) * (1 + num_chunks);
    if (num_chunks != numChunks(uncompressed_bytes) || header_bytes > block.size())
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "A GDeflate block of {} bytes holds {} chunks, where {} expanded bytes make {}",
            block.size(),
            num_chunks,
            uncompressed_bytes,
            numChunks(uncompressed_bytes));

    size_t at = header_bytes;
    for (size_t i = 0; i < num_chunks; ++i)
    {
        const GDeflateChunk chunk{
            .offset = at,
            .compressed_bytes = readUInt32(block.data() + sizeof(UInt32) * (1 + i)),
            .decompressed_offset = i * gdeflate_chunk_bytes,
            .decompressed_bytes = std::min(gdeflate_chunk_bytes, uncompressed_bytes - i * gdeflate_chunk_bytes),
        };
        if (at + chunk.compressed_bytes > block.size())
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS, "Chunk {} of a GDeflate block of {} bytes ends at {}", i, block.size(), at + chunk.compressed_bytes);
        on_chunk(chunk);
        at = alignUp(at + chunk.compressed_bytes);
    }
}

}

#endif
