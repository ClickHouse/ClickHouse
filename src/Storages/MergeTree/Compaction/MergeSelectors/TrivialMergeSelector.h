#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectors/IMergeSelector.h>

namespace DB
{

/** Go through partitions starting from the largest (in the number of parts).
  * Go through parts from left to right.
  * Find the first range of N parts where their level is not decreasing.
  * Then continue finding these ranges and find up to M of these ranges.
  * Choose a random one from them.
  */
class TrivialMergeSelector : public IMergeSelector
{
public:
    /// TODO: setup somewhere
    struct Settings
    {
        size_t num_parts_to_merge = 10;
        size_t num_ranges_to_choose = 100;
    };

    PartsRanges select(
        const PartsRanges & parts_ranges,
        const MergeSizes & max_merge_sizes,
        const RangeFilter & range_filter) const override;

private:
    const Settings settings;
};

}
