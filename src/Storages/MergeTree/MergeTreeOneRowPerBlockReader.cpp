#include <Storages/MergeTree/MergeTreeOneRowPerBlockReader.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ProfileEvents.h>
#include <Compression/CompressedReadBufferBase.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Formats/MarkInCompressedFile.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>

#include <algorithm>


namespace ProfileEvents
{
    extern const Event MergeTreeOneRowPerBlockReadRows;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Resizes the elements of the array column, without initializing them, and returns where they start.
char * resizeElements(IColumn & elements, size_t num_elements)
{
    auto resize = [&]<typename T>() -> char *
    {
        auto * column = typeid_cast<ColumnVector<T> *>(&elements);
        if (!column)
            return nullptr;
        column->getData().resize(num_elements);
        return reinterpret_cast<char *>(column->getData().data());
    };

    char * data = resize.template operator()<BFloat16>();
    if (!data)
        data = resize.template operator()<Float32>();
    if (!data)
        data = resize.template operator()<Float64>();
    if (!data)
        data = resize.template operator()<Int8>();
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column of array elements {}", elements.getName());
    return data;
}

}

MergeTreeOneRowPerBlockReader::MergeTreeOneRowPerBlockReader(OneRowPerBlockLayout layout_, UInt64 part_rows_, String path_for_logs_)
    : layout(std::move(layout_))
    , part_rows(part_rows_)
    , path_for_logs(std::move(path_for_logs_))
{
}

void MergeTreeOneRowPerBlockReader::readRange(
    MergeTreeReaderStream & stream, UInt64 first_row, UInt64 num_rows, const PaddedPODArray<UInt64> & selected_rows, IColumn & result)
{
    if (first_row + num_rows > part_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot read rows [{}, {}) from {} with {} rows",
            first_row, first_row + num_rows, path_for_logs, part_rows);

    const auto * selected_begin = std::lower_bound(selected_rows.begin(), selected_rows.end(), first_row);
    const auto * selected_end = std::lower_bound(selected_begin, selected_rows.end(), first_row + num_rows);
    const size_t num_selected = selected_end - selected_begin;

    auto & column_array = assert_cast<ColumnArray &>(result);
    auto & offsets = column_array.getOffsets();
    const size_t old_size = offsets.size();
    UInt64 offset = old_size ? offsets.back() : 0;
    offsets.resize(old_size + num_rows);
    const auto * next_selected = selected_begin;
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (next_selected != selected_end && *next_selected == first_row + i)
        {
            offset += layout.array_size;
            ++next_selected;
        }
        offsets[old_size + i] = offset;
    }

    auto & elements_column = column_array.getData();
    const size_t old_num_elements = elements_column.size();
    const size_t element_bytes = layout.row_bytes / layout.array_size;
    char * elements = resizeElements(elements_column, old_num_elements + num_selected * layout.array_size)
        + old_num_elements * element_bytes;

    for (const auto * row = selected_begin; row != selected_end; ++row)
    {
        readRow(stream, *row, elements);
        elements += layout.row_bytes;
    }

    ProfileEvents::increment(ProfileEvents::MergeTreeOneRowPerBlockReadRows, num_selected);
}

void MergeTreeOneRowPerBlockReader::readRow(MergeTreeReaderStream & stream, UInt64 row, char * to)
{
    const UInt64 offset = row * layout.block_bytes;

    /// A part written with other block sizes than the column has now has no block at the position of a row, which looks
    /// like corrupted data here. The exception says so, as nothing before the read can tell.
    auto explanation = [&]
    {
        return fmt::format(
            "While reading row {} from {} at offset {}, as one row per block (setting `merge_tree_use_one_row_per_block_layout`). "
            "Either the part is corrupted, or it was written with other values of `min_compress_block_size` and "
            "`max_compress_block_size` than the column has now. In the second case, merge the part or disable the setting",
            row, path_for_logs, offset);
    };

    std::optional<CompressedReadBufferBase::CompressedBlock> block;
    try
    {
        /// A seek to the block right after the last one read, or to a block in the read buffer, does not read anything.
        stream.seekToMark(MarkInCompressedFile{.offset_in_compressed_file = offset, .offset_in_decompressed_block = 0});
        block = stream.readCompressedBlock();
    }
    catch (Exception & e)
    {
        e.addMessage(explanation());
        throw;
    }

    if (!block)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read the block of row {} from {}: end of file", row, path_for_logs);

    /// Besides `NONE`, the codecs that store the data verbatim, e.g. `Quantized`, which only adds another stream.
    if (block->method != verbatim_method)
    {
        if (block->method != static_cast<UInt8>(CompressionMethodByte::NONE)
            && !CompressionCodecFactory::instance().get(block->method)->isNone())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "The block is compressed with method byte 0x{:02x}, expected a block stored without compression. {}",
                static_cast<unsigned>(block->method), explanation());
        verbatim_method = block->method;
    }

    if (block->decompressed_bytes != layout.row_bytes || block->payload.size() != layout.row_bytes)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "The block has {} bytes, {} decompressed, expected one row of {} bytes. {}",
            block->payload.size(), block->decompressed_bytes, layout.row_bytes, explanation());

    memcpy(to, block->payload.data(), layout.row_bytes);
}

}
