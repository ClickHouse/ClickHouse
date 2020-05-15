#include "CachedCompressedReadBuffer.h"

#include <Compression/LZ4_decompress_faster.h>
#include <IO/WriteHelpers.h>

#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

bool CachedCompressedReadBuffer::nextImpl()
{
    UInt128 key = cache->hash(path, file_pos);

    if (owned_cell = cache->get(key); !owned_cell)
    {
        initInput();
        file_in->seek(file_pos, SEEK_SET);

        UncompressedCell cell{}; //ordinary allocator

        size_t size_decompressed;
        size_t size_compressed_without_checksum;

        cell.compressed_size = readCompressedData(size_decompressed, size_compressed_without_checksum);
        cell.additional_bytes = codec->getAdditionalSizeAtTheEndOfBuffer();

        const size_t cell_overall_size =
            (size_decompressed + cell.additional_bytes) * (cell.compressed_size > 0);

        auto size_func = [cell_overall_size] { return cell_overall_size; };

        owned_cell = cache->getOrSet(key, std::move(size_func), [=, this, &cell](void * heap_address)
        {
            UncompressedCell other{}; //also ordinary allocator, tmp value that will pass the data and die.
            other.heap_storage = heap_address;

            if (cell.compressed_size)
            {
                other.compressed_size = cell.compressed_size;
                other.additional_bytes = cell.additional_bytes;
                other.data.resize(cell_overall_size);
                decompress(other.data.data(), size_decompressed, size_compressed_without_checksum);
            }

            return other;
        }).first;
    }

    if (owned_cell->data.size() == 0)
        return false;

    working_buffer = Buffer(owned_cell->data.data(),
                            owned_cell->data.data() + owned_cell->data.size() - owned_cell->additional_bytes);

    file_pos += owned_cell->compressed_size;

    return true;
}

void CachedCompressedReadBuffer::seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
{
    if (owned_cell &&
        offset_in_compressed_file == file_pos - owned_cell->compressed_size &&
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
            throw Exception(
                "Seek position is beyond the decompressed block"
                " (pos: "
                    + toString(offset_in_decompressed_block) +
                    ", block size: " + toString(working_buffer.size()) + ")",
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

        pos = working_buffer.begin() + offset_in_decompressed_block;
        bytes -= offset();
    }
}
}

