#pragma once

#include <Storages/MergeTree/MergeSelectors/MergeSelector.h>


namespace DB
{

class ComplexMergeSelector final : public IMergeSelector
{
public:
    struct Settings
    {
        /// Zero means unlimited. Can be overridden by the same merge tree setting.
        size_t max_parts_to_merge_at_once = 100;
    };

    explicit ComplexMergeSelector(const Settings & settings_) : settings(settings_) {}

    PartsRange select(
        const PartsRanges & parts_ranges,
        size_t max_total_size_to_merge) override;

private:
    const Settings settings;
};

}
