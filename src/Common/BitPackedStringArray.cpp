#include <Common/BitPackedStringArray.h>

namespace DB
{

BitPackedStringArray::BitPackedStringArray(std::span<const char> chars_, std::span<const UInt64> offsets_)
{
    chassert(offsets_.empty() ? chars_.empty() : offsets_.back() == chars_.size());

    chars.reserve_exact(chars_.size());
    chars.insert(chars_.begin(), chars_.end());
    offsets = BitPackedUInt64Array(offsets_);
}

BitPackedStringArray::BitPackedStringArray(const PaddedPODArray<UInt8> & chars_, const PaddedPODArray<UInt64> & offsets_)
    : BitPackedStringArray(
        std::span(reinterpret_cast<const char *>(chars_.data()), chars_.size()),
        std::span(offsets_.data(), offsets_.size()))
{
}

std::string_view BitPackedStringArray::get(size_t idx) const
{
    size_t begin = idx == 0 ? 0 : offsets.get(idx - 1);
    size_t end = offsets.get(idx);
    return {chars.data() + begin, end - begin};
}

size_t BitPackedStringArray::allocatedBytes() const
{
    return chars.allocated_bytes() + offsets.allocatedBytes();
}

}
