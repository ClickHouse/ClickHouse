#pragma once

#include <span>

#include <base/types.h>
#include <Common/PODArray.h>

namespace DB
{

/** In-memory bit-packed array of UInt64 values with O(1) random access.
  *
  * Uses the same technique as MarksInCompressedFile: the values are split into
  * blocks of VALUES_PER_BLOCK values. For each value, the difference
  * [value] - [min value in the block] is stored, bit-packed with the number
  * of bits required for the largest difference in the block.
  *
  * It significantly reduces memory usage when values at nearby positions
  * are close to each other, e.g. for steadily increasing offsets in a file.
  */
class BitPackedUInt64Array
{
public:
    BitPackedUInt64Array() = default;
    explicit BitPackedUInt64Array(std::span<const UInt64> values);

    UInt64 get(size_t idx) const;

    size_t size() const { return num_values; }
    bool empty() const { return num_values == 0; }
    size_t allocatedBytes() const;

private:
    struct BlockInfo
    {
        /// Min value in the block.
        UInt64 min_value = 0;
        /// Place in `packed` where this block starts.
        UInt64 bit_offset_in_packed_array = 0;
        /// How many bits each difference from min_value takes. Can be zero.
        UInt8 bits_per_value = 0;
    };

    static constexpr size_t VALUES_PER_BLOCK = 128;

    size_t num_values = 0;
    PODArray<BlockInfo> blocks;
    PODArray<UInt64> packed;
};

}
