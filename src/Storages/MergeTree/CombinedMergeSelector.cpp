#include <Storages/MergeTree/CombinedMergeSelector.h>

#include <cmath>


namespace DB
{


CombinedMergeSelector::PartsRange CombinedMergeSelector::select(
    const PartsRanges & parts_ranges,
    const size_t max_total_size_to_merge)
{
    const CombinedMergeSelector::PartsRange & partsRange = memoryMergeSelector.select(parts_ranges, max_total_size_to_merge);
    if (partsRange.empty())
        return simpleMergeSelector.select(parts_ranges, max_total_size_to_merge);
    else
        return partsRange;
}

}
