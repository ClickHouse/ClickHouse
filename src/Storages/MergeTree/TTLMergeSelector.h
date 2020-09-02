#pragma once

#include <Core/Types.h>
#include <Storages/MergeTree/MergeSelector.h>
#include <Storages/TTLDescription.h>

#include <map>


namespace DB
{

/** Merge selector, which is used to remove values with expired ttl.
  * It selects parts to merge by greedy algorithm:
  *  1. Finds part with the most earliest expired ttl and includes it to result.
  *  2. Tries to find the longest range of parts with expired ttl, that includes part from step 1.
  * Finally, merge selector updates TTL merge timer for the selected partition
  */
class ITTLMergeSelector : public IMergeSelector
{
public:
    using PartitionIdToTTLs = std::map<String, time_t>;

    ITTLMergeSelector(PartitionIdToTTLs & merge_due_times_, time_t current_time_, Int64 merge_cooldown_time_)
        : current_time(current_time_)
        , merge_due_times(merge_due_times_)
        , merge_cooldown_time(merge_cooldown_time_)
    {
    }

    PartsInPartition select(
        const Partitions & partitions,
        const size_t max_total_size_to_merge) override;

    virtual time_t getTTLForPart(const IMergeSelector::Part & part) const = 0;
    virtual bool isTTLAlreadySatisfied(const IMergeSelector::Part & part) const = 0;

protected:
    time_t current_time;

private:
    PartitionIdToTTLs & merge_due_times;
    Int64 merge_cooldown_time;
};


class TTLDeleteMergeSelector : public ITTLMergeSelector
{
public:
    using PartitionIdToTTLs = std::map<String, time_t>;

    TTLDeleteMergeSelector(PartitionIdToTTLs & merge_due_times_, time_t current_time_, Int64 merge_cooldown_time_, bool only_drop_parts_)
        : ITTLMergeSelector(merge_due_times_, current_time_, merge_cooldown_time_)
        , only_drop_parts(only_drop_parts_) {}

    time_t getTTLForPart(const IMergeSelector::Part & part) const override;

    bool isTTLAlreadySatisfied(const IMergeSelector::Part &) const override
    {
        return false;
    }

private:
    bool only_drop_parts;
};

class TTLRecompressMergeSelector : public ITTLMergeSelector
{
public:
    TTLRecompressMergeSelector(PartitionIdToTTLs & merge_due_times_, time_t current_time_, Int64 merge_cooldown_time_, const TTLDescriptions & recompression_ttls_)
        : ITTLMergeSelector(merge_due_times_, current_time_, merge_cooldown_time_)
        , recompression_ttls(recompression_ttls_)
    {}

    time_t getTTLForPart(const IMergeSelector::Part & part) const override;

    bool isTTLAlreadySatisfied(const IMergeSelector::Part & part) const override;
private:
    TTLDescriptions recompression_ttls;
};

}
