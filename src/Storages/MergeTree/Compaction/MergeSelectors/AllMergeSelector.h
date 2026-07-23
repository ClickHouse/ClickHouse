#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectors/IMergeSelector.h>

namespace DB
{

/// Select all parts within partition (having at least two parts) with minimum total size.
class AllMergeSelector : public IMergeSelector
{
public:
    /// Parameter max_total_size_to_merge is ignored.
    PartsRanges select(
        const PartsRanges & parts_ranges,
        const MergeSizes & max_merge_sizes,
        const RangeFilter & range_filter) const override;
};

}
