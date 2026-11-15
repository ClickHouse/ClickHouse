#include <Formats/MarkInCompressedFile.h>

#include <Common/BitHelpers.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String MarkInCompressedFile::toString() const
{
    return "(" + DB::toString(offset_in_compressed_file) + "," + DB::toString(offset_in_decompressed_block) + ")";
}

String MarkInCompressedFile::toStringWithRows(size_t rows_num) const
{
    return "(" + DB::toString(offset_in_compressed_file) + "," + DB::toString(offset_in_decompressed_block) + ","
        + DB::toString(rows_num) + ")";
}

std::shared_ptr<MarksInCompressedFile> MarksInCompressedFile::create(const PlainArray & marks)
{
    Builder builder(marks.size());
    builder.addAllMarks(marks.data(), marks.size());
    return builder.finish();
}

MarkInCompressedFile MarksInCompressedFile::get(size_t idx) const
{
    if (idx >= num_marks)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Mark index {} is out of range [0, {})",
            idx, num_marks);

    auto [block, offset] = lookUpMark(idx);
    size_t x = block->min_x + readBitsPacked64(packed.data(), offset, block->bits_for_x);
    size_t y = block->min_y + (readBitsPacked64(packed.data(), offset + block->bits_for_x, block->bits_for_y) << block->trailing_zero_bits_in_y);
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

MarksInCompressedFile::MarksInCompressedFile(
    size_t num_marks_,
    PODArray<BlockInfo, 4096, JemallocCacheAllocator> && blocks_,
    PODArray<UInt64, 4096, JemallocCacheAllocator> && packed_)
    : num_marks(num_marks_)
    , blocks(std::move(blocks_))
    , packed(std::move(packed_))
{
}

MarksInCompressedFile::Builder::Builder(size_t total_marks_)
    : total_marks(total_marks_)
    , blocks((total_marks_ + MARKS_PER_BLOCK - 1) / MARKS_PER_BLOCK, BlockInfo{})
{
}

void MarksInCompressedFile::Builder::addAllMarks(const MarkInCompressedFile * marks, size_t count)
{
    chassert(pending.empty() && marks_flushed == 0);
    chassert(count == total_marks);

    while (count > 0)
    {
        size_t chunk = std::min(MARKS_PER_BLOCK, count);
        flushBlock(marks, chunk);
        marks += chunk;
        count -= chunk;
    }
}

void MarksInCompressedFile::Builder::addMarks(const MarkInCompressedFile * marks, size_t count)
{
    chassert(marks_flushed + pending.size() + count <= total_marks);

    /// If there are pending marks from a previous call, fill up that block first.
    if (!pending.empty())
    {
        size_t marks_to_copy = std::min(MARKS_PER_BLOCK - pending.size(), count);
        pending.insert(pending.end(), marks, marks + marks_to_copy);
        marks += marks_to_copy;
        count -= marks_to_copy;

        if (pending.size() == MARKS_PER_BLOCK)
        {
            flushBlock(pending.data(), pending.size());
            pending.clear();
        }
    }

    /// Process full blocks directly from input without copying.
    while (count >= MARKS_PER_BLOCK)
    {
        flushBlock(marks, MARKS_PER_BLOCK);
        marks += MARKS_PER_BLOCK;
        count -= MARKS_PER_BLOCK;
    }

    /// Buffer remaining marks for the next call.
    if (count > 0)
        pending.insert(pending.end(), marks, marks + count);
}

void MarksInCompressedFile::Builder::flushBlock(const MarkInCompressedFile * data, size_t count)
{
    chassert(count > 0 && count <= MARKS_PER_BLOCK);

    size_t block_idx = marks_flushed / MARKS_PER_BLOCK;
    BlockInfo & block = blocks[block_idx];
    block.bit_offset_in_packed_array = packed_bits;

    /// Compute block metadata: min values, bit widths.
    size_t max_x = 0;
    size_t max_y = 0;
    for (size_t i = 0; i < count; ++i)
    {
        block.min_x = std::min(block.min_x, data[i].offset_in_compressed_file);
        max_x = std::max(max_x, data[i].offset_in_compressed_file);
        block.min_y = std::min(block.min_y, data[i].offset_in_decompressed_block);
        max_y = std::max(max_y, data[i].offset_in_decompressed_block);
        block.trailing_zero_bits_in_y
            = std::min(block.trailing_zero_bits_in_y, static_cast<UInt8>(getTrailingZeroBits(data[i].offset_in_decompressed_block)));
    }

    block.bits_for_x = static_cast<UInt8>(sizeof(size_t) * 8 - getLeadingZeroBits(max_x - block.min_x));
    block.bits_for_y
        = static_cast<UInt8>(sizeof(size_t) * 8 - getLeadingZeroBits((max_y - block.min_y) >> block.trailing_zero_bits_in_y));

    /// Grow packed array to fit new bits + 1 overallocation element for writeBitsPacked64 safety.
    size_t new_bits = count * (block.bits_for_x + block.bits_for_y);
    size_t new_packed_length = (packed_bits + new_bits + 63) / 64 + 1;
    if (new_packed_length > packed.size())
        packed.resize_fill(new_packed_length);

    /// Write bit-packed deltas.
    size_t bit_offset = packed_bits;
    for (size_t i = 0; i < count; ++i)
    {
        writeBitsPacked64(packed.data(), bit_offset, data[i].offset_in_compressed_file - block.min_x);
        writeBitsPacked64(
            packed.data(),
            bit_offset + block.bits_for_x,
            (data[i].offset_in_decompressed_block - block.min_y) >> block.trailing_zero_bits_in_y);
        bit_offset += block.bits_for_x + block.bits_for_y;
    }

    packed_bits += new_bits;
    marks_flushed += count;
}

std::shared_ptr<MarksInCompressedFile> MarksInCompressedFile::Builder::finish()
{
    /// Flush remaining buffered marks (last incomplete block).
    if (!pending.empty())
    {
        flushBlock(pending.data(), pending.size());
        pending.clear();
    }

    chassert(marks_flushed == total_marks);

    /// +1 overallocation element is needed by readBitsPacked64, but only for non-empty marks.
    size_t required_length = total_marks == 0 ? 0 : (packed_bits + 63) / 64 + 1;
    if (packed.size() < required_length)
        packed.resize_fill(required_length);

    /// Shrink packed to exact size so approximateMemoryUsage reports the true
    /// compressed size without power-of-two slack from incremental growth.
    PODArray<UInt64, 4096, JemallocCacheAllocator> exact_packed;
    exact_packed.reserve_exact(required_length);
    exact_packed.resize_fill(required_length);
    if (required_length > 0)
        memcpy(exact_packed.data(), packed.data(), required_length * sizeof(UInt64));
    packed = std::move(exact_packed);

    return std::shared_ptr<MarksInCompressedFile>(
        new MarksInCompressedFile(total_marks, std::move(blocks), std::move(packed)));
}

}
