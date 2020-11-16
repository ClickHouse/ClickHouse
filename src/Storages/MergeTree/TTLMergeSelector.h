#pragma once

#include <common/types.h>
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

    PartsRange select(
        const PartsRanges & parts_ranges,
        const size_t max_total_size_to_merge) override;

    /// Get TTL value for part, may depend on child type and some settings in
    /// constructor.
    virtual time_t getTTLForPart(const IMergeSelector::Part & part) const = 0;

    /// Sometimes we can check that TTL already satisfied using information
    /// stored in part and don't assign merge for such part.
    virtual bool isTTLAlreadySatisfied(const IMergeSelector::Part & part) const = 0;

protected:
    time_t current_time;

private:
    PartitionIdToTTLs & merge_due_times;
    Int64 merge_cooldown_time;
};


/// Select parts to merge using information about delete TTL. Depending on flag
/// only_drop_parts can use max or min TTL value.
class TTLDeleteMergeSelector : public ITTLMergeSelector
{
public:
    using PartitionIdToTTLs = std::map<String, time_t>;

    TTLDeleteMergeSelector(PartitionIdToTTLs & merge_due_times_, time_t current_time_, Int64 merge_cooldown_time_, bool only_drop_parts_)
        : ITTLMergeSelector(merge_due_times_, current_time_, merge_cooldown_time_)
        , only_drop_parts(only_drop_parts_) {}

    time_t getTTLForPart(const IMergeSelector::Part & part) const override;

    /// Delete TTL should be checked only by TTL time, there are no other ways
    /// to satisfy it.
    bool isTTLAlreadySatisfied(const IMergeSelector::Part &) const override;

private:
    bool only_drop_parts;
};

/// Select parts to merge using information about recompression TTL and
/// compression codec of existing parts.
class TTLRecompressMergeSelector : public ITTLMergeSelector
{
public:
    TTLRecompressMergeSelector(PartitionIdToTTLs & merge_due_times_, time_t current_time_, Int64 merge_cooldown_time_, const TTLDescriptions & recompression_ttls_)
        : ITTLMergeSelector(merge_due_times_, current_time_, merge_cooldown_time_)
        , recompression_ttls(recompression_ttls_)
    {}

    /// Return part min recompression TTL.
    time_t getTTLForPart(const IMergeSelector::Part & part) const override;

    /// Checks that part's codec is not already equal to required codec
    /// according to recompression TTL. It doesn't make sense to assign such
    /// merge.
    bool isTTLAlreadySatisfied(const IMergeSelector::Part & part) const override;
private:
    TTLDescriptions recompression_ttls;
};

}
