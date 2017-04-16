#pragma once

#include <cstddef>
#include <vector>


namespace DB
{


/** A pair of marks that defines the range of rows in a part. Specifically, the range has the form [begin * index_granularity, end * index_granularity).
  */
struct MarkRange
{
    std::size_t begin;
    std::size_t end;

    MarkRange() = default;
    MarkRange(const std::size_t begin, const std::size_t end) : begin{begin}, end{end} {}
};

using MarkRanges = std::vector<MarkRange>;


}
