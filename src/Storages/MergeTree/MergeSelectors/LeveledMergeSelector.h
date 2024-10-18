#pragma once

#include <Storages/MergeTree/MergeSelectors/MergeSelector.h>


/**

*/

namespace DB
{

class LeveledMergeSelector final : public IMergeSelector
{
public:
    struct Settings
    {
        size_t max_parts_to_merge_at_once = 10;
        size_t batch_size_step = 2;
        size_t windows_to_look_at_single_level = 10;
    };

    explicit LeveledMergeSelector(const Settings & settings_) : settings(settings_) {}

    IMergeSelector::PartsRange select(
        const IMergeSelector::PartsRanges & parts_ranges,
        size_t max_total_size_to_merge) override;

private:
    const Settings settings;
};

}
