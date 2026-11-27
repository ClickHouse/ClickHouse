#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectors/IMergeSelector.h>

namespace DB
{

/// Select all parts within partition (having at least two parts) with minimum total size.
class AllMergeSelector : public IMergeSelector
{
public:
    PartsRanges select(
        const PartsRanges & parts_ranges,
        const MergeConstraints & merge_constraints,
        const RangeFilter & range_filter) const override;
};

}
