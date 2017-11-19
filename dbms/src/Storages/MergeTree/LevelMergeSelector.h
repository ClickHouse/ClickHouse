#pragma once

#include <Storages/MergeTree/MergeSelector.h>


namespace DB
{

/** Select parts to merge based on its level.
  * Select first range of parts of parts_to_merge length with minimum level.
  */
class LevelMergeSelector : public IMergeSelector
{
public:
    struct Settings
    {
        size_t parts_to_merge = 10;
    };

    explicit LevelMergeSelector(const Settings & settings) : settings(settings) {}

    PartsInPartition select(
        const Partitions & partitions,
        const size_t max_total_size_to_merge) override;

private:
    const Settings settings;
};

}
