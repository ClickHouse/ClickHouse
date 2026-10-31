#pragma once

#include <cstddef>

namespace DB
{

struct ByteRange
{
    size_t offset = 0;
    size_t size = 0;
    [[nodiscard]] size_t end() const { return offset + size; }
    /// Whether this range shares at least one byte with `other` (half-open; touching ranges do not).
    [[nodiscard]] bool overlaps(ByteRange other) const { return offset < other.end() && other.offset < end(); }
};

}
