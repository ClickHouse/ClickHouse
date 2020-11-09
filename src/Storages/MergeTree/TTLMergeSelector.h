#pragma once

#include <Core/Types.h>
#include <Storages/MergeTree/MergeSelector.h>

#include <map>


namespace DB
{

/** Merge selector, which is used to remove values with expired ttl.
  * It selects parts to merge by greedy algorithm: 
  *  1. Finds part with the most earliest expired ttl and includes it to result.
  *  2. Tries to find the longest range of parts with expired ttl, that includes part from step 1.
  * Finally, merge selector updates TTL merge timer for the selected partition
  */
class TTLMergeSelector : public IMergeSelector
{
public:
    using PartitionIdToTTLs = std::map<String, time_t>;

    explicit TTLMergeSelector(PartitionIdToTTLs & merge_due_times_, time_t current_time_, Int64 merge_cooldown_time_, bool only_drop_parts_)
        : merge_due_times(merge_due_times_),
          current_time(current_time_),
          merge_cooldown_time(merge_cooldown_time_),
          only_drop_parts(only_drop_parts_) {}

    PartsInPartition select(
        const Partitions & partitions,
        const size_t max_total_size_to_merge) override;

private:
    PartitionIdToTTLs & merge_due_times;
    time_t current_time;
    Int64 merge_cooldown_time;
    bool only_drop_parts;
};

}
