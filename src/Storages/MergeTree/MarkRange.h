#pragma once

#include <cstddef>
#include <deque>
#include <set>

#include <IO/WriteBuffer.h>

namespace DB
{


/** A pair of marks that defines the range of rows in a part. Specifically,
 * the range has the form [begin * index_granularity, end * index_granularity).
 */
struct MarkRange
{
    size_t begin;
    size_t end;

    bool operator==(const MarkRange & rhs) const;

    bool operator<(const MarkRange & rhs) const;
};

using MarkRanges = std::deque<MarkRange>;

/** Get max range.end from ranges.
 */
size_t getLastMark(const MarkRanges & ranges);

}
