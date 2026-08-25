#include <Formats/MarkInCompressedFile.h>

#include <Common/BitHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

String MarkInCompressedFile::toString() const
{
    return "(" + DB::toString(offset_in_compressed_file) + "," + DB::toString(offset_in_decompressed_block) + ")";
}

String MarkInCompressedFile::toStringWithRows(size_t rows_num) const
{
    return "(" + DB::toString(offset_in_compressed_file) + "," + DB::toString(offset_in_decompressed_block) + ","
        + DB::toString(rows_num) + ")";
}

// Write a range of bits in a bit-packed array.
// The array must be overallocated by one element.
// The bit range must be pre-filled with zeros.
void writeBits(UInt64 * dest, size_t bit_offset, UInt64 value)
{
    size_t mod = bit_offset % 64;
    dest[bit_offset / 64] |= value << mod;
    if (mod)
        dest[bit_offset / 64 + 1] |= value >> (64 - mod);
}

// The array must be overallocated by one element.
UInt64 readBits(const UInt64 * src, size_t bit_offset, size_t num_bits)
{
    size_t mod = bit_offset % 64;
    UInt64 value = src[bit_offset / 64] >> mod;
    if (mod)
        value |= src[bit_offset / 64 + 1] << (64 - mod);
    return value & maskLowBits<UInt64>(num_bits);
}

MarksInCompressedFile::MarksInCompressedFile(const PlainArray & marks)
    : num_marks(marks.size()), blocks((marks.size() + MARKS_PER_BLOCK - 1) / MARKS_PER_BLOCK, BlockInfo{})
{
    if (num_marks == 0)
    {
        return;
    }

    // First pass: calculate layout of all blocks and total memory required.
    size_t packed_bits = 0;
    for (size_t block_idx = 0; block_idx < blocks.size(); ++block_idx)
    {
        BlockInfo & block = blocks[block_idx];
        block.bit_offset_in_packed_array = packed_bits;

        size_t max_x = 0;
        size_t max_y = 0;
        size_t num_marks_in_this_block = std::min(MARKS_PER_BLOCK, num_marks - block_idx * MARKS_PER_BLOCK);
        for (size_t i = 0; i < num_marks_in_this_block; ++i)
        {
            const auto & mark = marks[block_idx * MARKS_PER_BLOCK + i];
            block.min_x = std::min(block.min_x, mark.offset_in_compressed_file);
            max_x = std::max(max_x, mark.offset_in_compressed_file);
            block.min_y = std::min(block.min_y, mark.offset_in_decompressed_block);
            max_y = std::max(max_y, mark.offset_in_decompressed_block);

            block.trailing_zero_bits_in_y
                = std::min(block.trailing_zero_bits_in_y, static_cast<UInt8>(getTrailingZeroBits(mark.offset_in_decompressed_block)));
        }

        block.bits_for_x = sizeof(size_t) * 8 - getLeadingZeroBits(max_x - block.min_x);
        block.bits_for_y = sizeof(size_t) * 8 - getLeadingZeroBits((max_y - block.min_y) >> block.trailing_zero_bits_in_y);
        packed_bits += num_marks_in_this_block * (block.bits_for_x + block.bits_for_y);
    }

    // Overallocate by +1 element to let the bit packing/unpacking do less bounds checking.
    size_t packed_length = (packed_bits + 63) / 64 + 1;
    packed.reserve_exact(packed_length);
    packed.resize_fill(packed_length);

    // Second pass: write out the packed marks.
    for (size_t idx = 0; idx < num_marks; ++idx)
    {
        const auto & mark = marks[idx];
        auto [block, offset] = lookUpMark(idx);
        writeBits(packed.data(), offset, mark.offset_in_compressed_file - block->min_x);
        writeBits(
            packed.data(),
            offset + block->bits_for_x,
            (mark.offset_in_decompressed_block - block->min_y) >> block->trailing_zero_bits_in_y);
    }
}

MarkInCompressedFile MarksInCompressedFile::get(size_t idx) const
{
    auto [block, offset] = lookUpMark(idx);
    size_t x = block->min_x + readBits(packed.data(), offset, block->bits_for_x);
    size_t y = block->min_y + (readBits(packed.data(), offset + block->bits_for_x, block->bits_for_y) << block->trailing_zero_bits_in_y);
    return MarkInCompressedFile{.offset_in_compressed_file = x, .offset_in_decompressed_block = y};
}

std::tuple<const MarksInCompressedFile::BlockInfo *, size_t> MarksInCompressedFile::lookUpMark(size_t idx) const
{
    size_t block_idx = idx / MARKS_PER_BLOCK;
    const BlockInfo & block = blocks[block_idx];
    size_t offset = block.bit_offset_in_packed_array + (idx - block_idx * MARKS_PER_BLOCK) * (block.bits_for_x + block.bits_for_y);
    return {&block, offset};
}

size_t MarksInCompressedFile::approximateMemoryUsage() const
{
    return sizeof(*this) + blocks.allocated_bytes() + packed.allocated_bytes();
}

}
