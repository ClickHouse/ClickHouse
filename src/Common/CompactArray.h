#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <base/unaligned.h>

#include <limits>
#include <span>

namespace DB
{

/** Compact array for data storage, size `content_width`, in bits, of which is
  * less than one byte. Instead of storing each value in a separate
  * bytes, which leads to a waste of 37.5% of the space for content_width = 5, CompactArray stores
  * adjacent `content_width`-bit values in the byte array, that is actually CompactArray
  * simulates an array of `content_width`-bit values.
  */
template <typename BucketIndex, UInt8 content_width, size_t bucket_count>
class CompactArray final
{
public:
    CompactArray() = default;

    UInt8 ALWAYS_INLINE operator[](BucketIndex bucket_index) const
    {
        return get(bucket_index);
    }

    /// Cells are accessed through an unaligned 16-bit word, which makes the access branchless.
    /// The trailing padding byte of the bitset keeps the word in bounds for every bucket.
    UInt8 ALWAYS_INLINE get(BucketIndex bucket_index) const
    {
        chassert(bucket_index < bucket_count);

        size_t bit = static_cast<size_t>(bucket_index) * content_width;
        UInt16 word = unalignedLoadLittleEndian<UInt16>(&bitset[bit / 8]);
        return (word >> (bit % 8)) & ((1 << content_width) - 1);
    }

    void ALWAYS_INLINE set(BucketIndex bucket_index, UInt8 content)
    {
        chassert(bucket_index < bucket_count);

        size_t bit = static_cast<size_t>(bucket_index) * content_width;
        constexpr UInt16 mask = (1 << content_width) - 1;
        UInt16 word = unalignedLoadLittleEndian<UInt16>(&bitset[bit / 8]);
        word = static_cast<UInt16>((word & ~(mask << (bit % 8))) | (static_cast<UInt16>(content) << (bit % 8)));
        unalignedStoreLittleEndian<UInt16>(&bitset[bit / 8], word);
    }

    /// The serialized state is the bitset without the trailing padding byte.
    /// The serialization format must not be changed.
    std::span<const UInt8> getSerializableState() const { return {bitset, BITSET_SIZE}; }
    std::span<UInt8> getSerializableState() { return {bitset, BITSET_SIZE}; }
    static constexpr size_t serializedSize() { return BITSET_SIZE; }

private:
    /// number of bytes in bitset
    static constexpr size_t BITSET_SIZE = (bucket_count * content_width + 7) / 8;

    /// The last byte is padding for the branchless 16-bit access; it stays zero and is not serialized.
    UInt8 bitset[BITSET_SIZE + 1] = { 0 };

    /// Checks
    static_assert((content_width > 0) && (content_width < 8), "Invalid parameter value");
    static_assert(bucket_count <= (std::numeric_limits<size_t>::max() / content_width), "Invalid parameter value");
};

}
