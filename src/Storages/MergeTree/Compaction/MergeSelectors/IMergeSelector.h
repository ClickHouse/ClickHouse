#pragma once

#include <Storages/MergeTree/Compaction/PartProperties.h>

#include <limits>

namespace DB
{

struct MergeConstraint
{
    static constexpr size_t unlimited_parts = std::numeric_limits<size_t>::max();

    MergeConstraint(size_t max_size_bytes_, size_t max_size_rows_, size_t max_parts_ = unlimited_parts)
      : max_size_bytes(max_size_bytes_), max_size_rows(max_size_rows_), max_parts(max_parts_)
    {
    }

    size_t max_size_bytes;
    size_t max_size_rows;

    /// The maximum number of source parts in one merge that the server executing it can afford,
    /// see `CompactionStatistics::getMaxPartsToMergeAtOnceForMemory`. It is at least two: a merge of fewer
    /// parts makes no progress. Honored by the selectors of regular merges (`SimpleMergeSelector`,
    /// `TrivialMergeSelector`), on top of their own settings; not by the selectors of merges with TTL
    /// or by the ones that merge what the user explicitly asked for.
    size_t max_parts;
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
    [[nodiscard]] virtual PartsRanges select(
        const PartsRanges & parts_ranges,
        const MergeConstraints & merge_constraints,
        const RangeFilter & range_filter) const = 0;

    virtual ~IMergeSelector() = default;
};

using MergeSelectorPtr = std::shared_ptr<IMergeSelector>;

}
