#pragma once

#include <Storages/MergeTree/MergeSelector.h>


namespace DB
{

/** Merge selector, which is used to remove values with expired ttl.
*   It selects parts to merge by greedy algorithm: 
*    1. Finds part with the most earliest expired ttl and includes it to result.
*    2. Tries to find the longest range of parts with expired ttl, that includes part from step 1.
*/
class TTLMergeSelector : public IMergeSelector
{
public:
    PartsInPartition select(
        const Partitions & partitions,
        const size_t max_total_size_to_merge) override;
};

}
