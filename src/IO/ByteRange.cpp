#include <IO/ByteRange.h>

#include <algorithm>

namespace DB
{

bool ByteRange::overlaps(ByteRange other) const
{
    return offset < other.end() && other.offset < end();
}

ByteRange ByteRange::intersect(ByteRange other) const
{
    const size_t lo = std::max(offset, other.offset);
    const size_t hi = std::min(end(), other.end());
    return lo < hi ? ByteRange{lo, hi - lo} : ByteRange{};
}

std::pair<ByteRange, ByteRange> ByteRange::subtract(ByteRange other) const
{
    const auto common = intersect(other);
    if (common.size == 0)
        return {*this, {}};
    return {ByteRange{offset, common.offset - offset}, ByteRange{common.end(), end() - common.end()}};
}

}
