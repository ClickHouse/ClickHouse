#pragma once

#include <Storages/MergeTree/MergeSelector.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <common/logger_useful.h>


namespace DB
{
/**
 *  1. only handle parts in memory.
 *  2. aggressive, merge as many as possible in one merging operation.
 *  3. modified from LevelMergeSelector.
 */
class MemoryMergeSelector : public IMergeSelector
{
public:
    struct Settings
    {
        size_t min_parts_to_merge = 10;
        size_t max_parts_to_merge = 100;

        Settings() = default;

        Settings(MergeTreeSettingsPtr mergeTreeSettings)
        {
            min_parts_to_merge = mergeTreeSettings->min_parts_to_merge;
            max_parts_to_merge = mergeTreeSettings->max_parts_to_merge;
        }
    };

    explicit MemoryMergeSelector(const Settings & settings_) : settings(settings_), log(&Poco::Logger::get("MemoryMergeSelector")) { }

    PartsRange select(const PartsRanges & parts_ranges, const size_t max_total_size_to_merge) override;

    void dumpPartsInfo(const MemoryMergeSelector::PartsRange & parts, String prefix);

private:
    const Settings settings;
    Poco::Logger * log;
};

}
