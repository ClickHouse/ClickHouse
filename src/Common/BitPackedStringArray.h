#pragma once

#include <span>
#include <string_view>

#include <base/types.h>
#include <Common/BitPackedUInt64Array.h>
#include <Common/PODArray.h>

namespace DB
{

/** In-memory array of strings with O(1) random access, decomposed into
  * a contiguous array of characters and an array of end offsets.
  *
  * Compared to ColumnString the offsets are stored bit-packed
  * (see BitPackedUInt64Array), which significantly reduces
  * memory usage for short strings.
  */
class BitPackedStringArray
{
public:
    BitPackedStringArray() = default;
    BitPackedStringArray(std::span<const char> chars_, std::span<const UInt64> offsets_);
    BitPackedStringArray(const PaddedPODArray<UInt8> & chars_, const PaddedPODArray<UInt64> & offsets_);

    std::string_view get(size_t idx) const;

    size_t size() const { return offsets.size(); }
    bool empty() const { return offsets.empty(); }
    size_t allocatedBytes() const;

private:
    PODArray<char> chars;
    /// End offsets of strings in chars: i-th string is chars[offsets[i - 1], offsets[i]).
    BitPackedUInt64Array offsets;
};

}
