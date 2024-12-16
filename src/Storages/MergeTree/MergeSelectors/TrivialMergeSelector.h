#pragma once

#include <Storages/MergeTree/MergeSelectors/MergeSelector.h>


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
    struct Settings
    {
        size_t num_parts_to_merge = 10;
        size_t num_ranges_to_choose = 100;
    };

    PartsRange select(
        const PartsRanges & parts_ranges,
        size_t max_total_size_to_merge) override;

private:
    const Settings settings;
};

}
