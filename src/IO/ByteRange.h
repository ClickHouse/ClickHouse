#pragma once

#include <cstddef>
#include <utility>

namespace DB
{

struct ByteRange
{
    size_t offset = 0;
    size_t size = 0;
    size_t end() const { return offset + size; }
    /// Whether this range shares at least one byte with `other` (half-open; touching ranges do not).
    bool overlaps(ByteRange other) const;
    /// The common part of this range and `other`; empty if they do not overlap.
    ByteRange intersect(ByteRange other) const;
    /// The parts of this range outside `other`: before it and after it. Either can be empty.
    std::pair<ByteRange, ByteRange> subtract(ByteRange other) const;
};

}
