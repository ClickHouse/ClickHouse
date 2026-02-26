#pragma once

#include <Storages/MergeTree/Compaction/PartProperties.h>

namespace DB
{

/** Interface of algorithm to select data parts to merge
 *   (merge is also known as "compaction").
  * Following properties depend on it:
  *
  * 1. Number of data parts at some moment in time.
  *    If parts are merged frequently, then data will be represented by lower number of parts, in average,
  *     but with cost of higher write amplification.
  *
  * 2. Write amplification ratio: how much times, on average, source data was written
  *     (during initial writes and followed merges).
  *
  * Number of parallel merges are controlled outside of scope of this interface.
  */
class IMergeSelector
{
public:
    using RangeFilter = std::function<bool(PartsRangeView)>;

    /** Function could be called at any frequency and it must decide, should you do any merge at all.
      * If better not to do any merge, it returns empty result.
      */
    virtual PartsRange select(
        const PartsRanges & parts_ranges,
        size_t max_total_size_to_merge,
        RangeFilter range_filter) const = 0;

    virtual ~IMergeSelector() = default;
};

}
