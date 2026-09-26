#pragma once

#include <Storages/MergeTree/Compaction/PartProperties.h>

namespace DB
{

struct MergeConstraint
{
    MergeConstraint(size_t max_size_bytes_, size_t max_size_rows_)
      : max_size_bytes(max_size_bytes_), max_size_rows(max_size_rows_)
    {
    }

    size_t max_size_bytes;
    size_t max_size_rows;
};

using MergeConstraints = std::span<const MergeConstraint>;

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
      *
      * @param parts_ranges Initial parts ranges returned from parts collector.
      * @param merge_constraints Requested constraints for merges.
      * @param range_filter Additional constraints on returned ranges.
      * @return Selected ranges up to merge_constraints.size() but may be less if some of them did not fit into merge constraints.
      */
    virtual PartsRanges select(
        const PartsRanges & parts_ranges,
        const MergeConstraints & merge_constraints,
        const RangeFilter & range_filter) const = 0;

    virtual ~IMergeSelector() = default;
};

using MergeSelectorPtr = std::shared_ptr<IMergeSelector>;

}
