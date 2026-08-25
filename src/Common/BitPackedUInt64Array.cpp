#include <Common/BitPackedUInt64Array.h>
#include <Common/BitHelpers.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BitPackedUInt64Array::BitPackedUInt64Array(std::span<const UInt64> values)
    : num_values(values.size())
{
    if (num_values == 0)
        return;

    size_t num_blocks = (num_values + VALUES_PER_BLOCK - 1) / VALUES_PER_BLOCK;
    blocks.reserve_exact(num_blocks);
    blocks.resize_fill(num_blocks);

    /// First pass: calculate the layout of all blocks and the total memory required.
    size_t packed_bits = 0;
    for (size_t block_idx = 0; block_idx < num_blocks; ++block_idx)
    {
        size_t block_begin = block_idx * VALUES_PER_BLOCK;
        size_t block_end = std::min(block_begin + VALUES_PER_BLOCK, num_values);

        UInt64 min_value = values[block_begin];
        UInt64 max_value = values[block_begin];

        for (size_t i = block_begin + 1; i < block_end; ++i)
        {
            min_value = std::min(min_value, values[i]);
            max_value = std::max(max_value, values[i]);
        }

        BlockInfo & block = blocks[block_idx];
        block.min_value = min_value;
        block.bit_offset_in_packed_array = packed_bits;
        block.bits_per_value = static_cast<UInt8>(sizeof(UInt64) * 8 - getLeadingZeroBits(max_value - min_value));

        packed_bits += (block_end - block_begin) * block.bits_per_value;
    }

    /// Overallocate by +1 element to let the bit packing/unpacking do less bounds checking.
    size_t packed_length = (packed_bits + 63) / 64 + 1;
    packed.reserve_exact(packed_length);
    packed.resize_fill(packed_length);

    /// Second pass: write out the packed values.
    for (size_t idx = 0; idx < num_values; ++idx)
    {
        const BlockInfo & block = blocks[idx / VALUES_PER_BLOCK];
        size_t bit_offset = block.bit_offset_in_packed_array + (idx % VALUES_PER_BLOCK) * block.bits_per_value;
        writeBitsPacked64(packed.data(), bit_offset, values[idx] - block.min_value);
    }
}

UInt64 BitPackedUInt64Array::get(size_t idx) const
{
    if (idx >= num_values)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index {} is out of range [0, {})", idx, num_values);

    const BlockInfo & block = blocks[idx / VALUES_PER_BLOCK];
    size_t bit_offset = block.bit_offset_in_packed_array + (idx % VALUES_PER_BLOCK) * block.bits_per_value;
    return block.min_value + readBitsPacked64(packed.data(), bit_offset, block.bits_per_value);
}

size_t BitPackedUInt64Array::allocatedBytes() const
{
    return blocks.allocated_bytes() + packed.allocated_bytes();
}

}
