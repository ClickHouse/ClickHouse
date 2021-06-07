#pragma once

#include <Storages/MergeTree/MergeSelector.h>


namespace DB
{

/// Select all parts within partition (having at least two parts) with minimum total size.
class AllMergeSelector : public IMergeSelector
{
public:
    /// Parameter max_total_size_to_merge is ignored.
    PartsRange select(
        const PartsRanges & parts_ranges,
        const size_t max_total_size_to_merge) override;
};

}
