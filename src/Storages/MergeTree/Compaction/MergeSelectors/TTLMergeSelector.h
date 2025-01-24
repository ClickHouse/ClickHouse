#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectors/IMergeSelector.h>
#include <Storages/TTLDescription.h>

namespace DB
{

using PartitionIdToTTLs = std::map<String, time_t>;

/** Merge selector, which is used to remove values with expired ttl.
  * It selects parts to merge by greedy algorithm:
  *  1. Finds part with the most earliest expired ttl and includes it to result.
  *  2. Tries to find the longest range of parts with expired ttl, that includes part from step 1.
  */
class ITTLMergeSelector : public IMergeSelector
{
public:
    ITTLMergeSelector(const PartitionIdToTTLs & merge_due_times_, time_t current_time_);

    PartsRange select(
        const PartsRanges & parts_ranges,
        size_t max_total_size_to_merge,
        RangeFilter range_filter) const override;

protected:
    /// Get TTL value for part, may depend on child type and some settings in constructor.
    virtual time_t getTTLForPart(const PartProperties & part) const = 0;

    /// Returns true if part can be used during ranges building process.
    virtual bool canConsiderPart(const PartProperties & part) const = 0;

private:
    using RangesIterator = PartsRanges::const_iterator;
    using PartsIterator = PartsRange::const_iterator;
    struct CenterPosition
    {
        RangesIterator range;
        PartsIterator center;
    };

    bool needToPostponePartition(const std::string & partition_id) const;
    std::optional<CenterPosition> findCenter(const PartsRanges & parts_ranges) const;
    PartsIterator findLeftRangeBorder(PartsIterator left, PartsIterator begin, size_t & usable_memory) const;
    PartsIterator findRightRangeBorder(PartsIterator right, PartsIterator end, size_t & usable_memory) const;

    const time_t current_time;
    const PartitionIdToTTLs & merge_due_times;
};

/// Select parts that must be fully deleted because of ttl for part.
class TTLPartDeleteMergeSelector : public ITTLMergeSelector
{
public:
    explicit TTLPartDeleteMergeSelector(const PartitionIdToTTLs & merge_due_times_, time_t current_time_);

private:
    time_t getTTLForPart(const PartProperties & part) const override;

    /// Actually does not check anything. Allows to use any part.
    bool canConsiderPart(const PartProperties & part) const override;
};

/// Select parts that has some expired ttls.
class TTLRowDeleteMergeSelector : public ITTLMergeSelector
{
public:
    explicit TTLRowDeleteMergeSelector(const PartitionIdToTTLs & merge_due_times_, time_t current_time_);

private:
    time_t getTTLForPart(const PartProperties & part) const override;

    /// Checks that part has at least one unfinished ttl. Because if all ttls
    /// are finished for part - it will be considered by TTLPartDeleteMergeSelector.
    bool canConsiderPart(const PartProperties & part) const override;
};

/// Select parts to merge using information about recompression TTL and compression codec of existing parts.
class TTLRecompressMergeSelector : public ITTLMergeSelector
{
public:
    explicit TTLRecompressMergeSelector(const PartitionIdToTTLs & merge_due_times_, time_t current_time_);

private:
    /// Return part min recompression TTL.
    time_t getTTLForPart(const PartProperties & part) const override;

    /// Checks that part's codec is not already equal to required codec
    /// according to recompression TTL. It doesn't make sense to assign such merge.
    bool canConsiderPart(const PartProperties & part) const override;
};

}
