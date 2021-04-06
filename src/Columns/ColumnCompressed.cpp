#include <Columns/ColumnCompressed.h>

#pragma GCC diagnostic ignored "-Wold-style-cast"

#include <lz4.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
}


std::shared_ptr<Memory<>> ColumnCompressed::compressBuffer(const void * data, size_t data_size, bool always_compress)
{
    size_t max_dest_size = LZ4_COMPRESSBOUND(data_size);

    if (max_dest_size > std::numeric_limits<int>::max())
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress column of size {}", formatReadableSizeWithBinarySuffix(data_size));

    Memory<> compressed(max_dest_size);

    auto compressed_size = LZ4_compress_default(
        reinterpret_cast<const char *>(data),
        compressed.data(),
        data_size,
        max_dest_size);

    if (compressed_size <= 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress column");

    /// If compression is inefficient.
    if (!always_compress && static_cast<size_t>(compressed_size) * 2 > data_size)
        return {};

    /// Shrink to fit.
    auto shrank = std::make_shared<Memory<>>(compressed_size);
    memcpy(shrank->data(), compressed.data(), compressed_size);

    return shrank;
}


void ColumnCompressed::decompressBuffer(
    const void * compressed_data, void * decompressed_data, size_t compressed_size, size_t decompressed_size)
{
    auto processed_size = LZ4_decompress_safe(
        reinterpret_cast<const char *>(compressed_data),
        reinterpret_cast<char *>(decompressed_data),
        compressed_size,
        decompressed_size);

    if (processed_size <= 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress column");
}

}
