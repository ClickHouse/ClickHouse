#include <GPU/GPUGDeflate.h>

#if USE_GPU

#include <GPU/BitUnpack.cuh>

#include <Common/Exception.h>
#include <base/types.h>

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

/// Set in the number of chunks of a block whose chunks have descriptors of `packed_descriptor_bytes`.
constexpr UInt32 packed_flag = UInt32{1} << 31;
constexpr size_t plain_descriptor_bytes = sizeof(UInt32);
constexpr size_t packed_descriptor_bytes = 16;

size_t alignUp(size_t bytes, size_t alignment)
{
    return (bytes + alignment - 1) / alignment * alignment;
}

size_t numChunks(size_t uncompressed_bytes)
{
    return (uncompressed_bytes + gdeflate_chunk_bytes - 1) / gdeflate_chunk_bytes;
}

size_t headerBytes(size_t num_chunks, bool packed)
{
    return sizeof(UInt32) + num_chunks * (packed ? packed_descriptor_bytes : plain_descriptor_bytes);
}

template <typename T>
T readAt(const char * at)
{
    T value;
    memcpy(&value, at, sizeof(value));
    return value;
}

}

void forEachGDeflateChunk(std::string_view block, size_t uncompressed_bytes, const std::function<void(const GDeflateChunk &)> & on_chunk)
{
    if (block.size() < sizeof(UInt32))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "A GDeflate block of {} bytes", block.size());

    const UInt32 header = readAt<UInt32>(block.data());
    const bool packed = header & packed_flag;
    const size_t num_chunks = header & ~packed_flag;
    if (num_chunks != numChunks(uncompressed_bytes) || headerBytes(num_chunks, packed) > block.size())
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "A GDeflate block of {} bytes holds {} chunks, where {} expanded bytes make {}",
            block.size(),
            num_chunks,
            uncompressed_bytes,
            numChunks(uncompressed_bytes));

    size_t at = headerBytes(num_chunks, packed);
    for (size_t i = 0; i < num_chunks; ++i)
    {
        const char * const descriptor = block.data() + headerBytes(i, packed);
        GDeflateChunk chunk{
            .offset = at,
            .compressed_bytes = readAt<UInt32>(descriptor),
            .decompressed_bytes = std::min(gdeflate_chunk_bytes, uncompressed_bytes - i * gdeflate_chunk_bytes),
        };
        chunk.expanded_bytes = chunk.decompressed_bytes;
        if (packed && descriptor[4] != 0)
        {
            chunk.packed_bits = static_cast<unsigned char>(descriptor[4]);
            chunk.element_size = static_cast<unsigned char>(descriptor[5]);
            chunk.reference = readAt<UInt64>(descriptor + 8);
            if (chunk.packed_bits > max_packed_bits
                || (chunk.element_size != 1 && chunk.element_size != 2 && chunk.element_size != 4 && chunk.element_size != 8)
                || chunk.decompressed_bytes % chunk.element_size != 0)
                throw Exception(
                    ErrorCodes::CANNOT_DECOMPRESS,
                    "Chunk {} of a GDeflate block packs integers of {} bytes in {} bits",
                    i,
                    chunk.element_size,
                    chunk.packed_bits);
            chunk.expanded_bytes = (chunk.decompressed_bytes / chunk.element_size * chunk.packed_bits + 7) / 8;
        }

        if (at + chunk.compressed_bytes > block.size())
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS, "Chunk {} of a GDeflate block of {} bytes ends at {}", i, block.size(), at + chunk.compressed_bytes);
        on_chunk(chunk);
        at = alignUp(at + chunk.compressed_bytes, chunk_alignment);
    }
}

}

#endif
