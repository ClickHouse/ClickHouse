#pragma once

#include <cstddef>
#include <vector>


namespace DB
{


/** A pair of marks that defines the range of rows in a part. Specifically, the range has the form [begin * index_granularity, end * index_granularity).
  */
struct MarkRange
{
    size_t begin;
    size_t end;

    MarkRange() = default;
    MarkRange(const size_t begin, const size_t end) : begin{begin}, end{end} {}
};

using MarkRanges = std::vector<MarkRange>;


}
