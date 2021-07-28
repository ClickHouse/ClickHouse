#pragma once

#include <Storages/MergeTree/MergeSelector.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MemoryMergeSelector.h>
#include <Storages/MergeTree/SimpleMergeSelector.h>


namespace DB
{

/**
 *  1. combine Memory and Simple merge-selector
 *  2. first memory, then disk
 */
class CombinedMergeSelector : public IMergeSelector
{
public:
    explicit CombinedMergeSelector(const MergeTreeSettingsPtr settings_) :
        memoryMergeSelector(settings_), simpleMergeSelector(settings_)
    {

    }

    PartsRange select(
        const PartsRanges & parts_ranges,
        const size_t max_total_size_to_merge) override;

private:
    MemoryMergeSelector memoryMergeSelector;
    SimpleMergeSelector simpleMergeSelector;
};

}
