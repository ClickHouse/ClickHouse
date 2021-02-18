#include <Common/Arena.h>
#include <Columns/ColumnCompressed.h>

#include <Compression/LZ4_decompress_faster.h>

#pragma GCC diagnostic ignored "-Wold-style-cast"
#include <lz4.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
}


StringRef ColumnCompressed::compressBuffer(const ArenaPtr & arena, const void * data, size_t data_size, bool always_compress)
{
    size_t max_dest_size = LZ4_COMPRESSBOUND(data_size);

    if (max_dest_size > std::numeric_limits<int>::max())
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress column of size {}", formatReadableSizeWithBinarySuffix(data_size));

    size_t allocated_size = max_dest_size + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER;
    char * allocated = arena->alloc(allocated_size);

    auto compressed_size = LZ4_compress_default(
        reinterpret_cast<const char *>(data),
        allocated,
        data_size,
        max_dest_size);

    if (compressed_size <= 0)
    {
        arena->rollback(allocated_size);
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress column");
    }

    /// If compression is inefficient.
    if (!always_compress && static_cast<size_t>(compressed_size) * 2 > data_size)
    {
        arena->rollback(allocated_size);
        return {};
    }

    /// Shrink to fit
    arena->rollback(allocated_size - compressed_size);
    return {allocated, size_t(compressed_size)};
}


void ColumnCompressed::decompressBuffer(
    const void * compressed_data,
    void * decompressed_data,
    size_t compressed_size,
    size_t decompressed_size,
    LZ4::PerformanceStatistics & statistics)
{
    LZ4::decompress(
        reinterpret_cast<const char *>(compressed_data),
        reinterpret_cast<char *>(decompressed_data),
        compressed_size,
        decompressed_size,
        statistics);
}

}
