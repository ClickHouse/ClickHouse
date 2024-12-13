#pragma once

#include <concepts>
#include <initializer_list>
#include <utility>
#include <vector>

#include <base/insertAtEnd.h>

#include <Storages/MergeTree/Compaction/PartsCollectors/IPartsCollector.h>

namespace DB
{

class PartsCollectorCombinator : public IPartsCollector
{
public:
    template <std::same_as<PartsCollectorPtr>... Collectors>
    explicit PartsCollectorCombinator(Collectors&&... part_collectors)
        : collectors(std::initializer_list{std::forward<Collectors>(part_collectors)...})
    {
    }

    PartsRanges collectPartsToUse(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr & storage_policy,
        const time_t & current_time,
        const std::optional<PartitionIdsHint> & partitions_hint) const override
    {
        PartsRanges ranges;

        for (const auto & collector : collectors)
        {
            auto collected_ranges = collector->collectPartsToUse(partitions_hint, metadata_snapshot, storage_policy, current_time, partitions_hint);
            insertAtEnd(ranges, std::move(collected_ranges));
        }

        /// TODO (michicosun): Add sanity check for intersecting parts here

        return ranges;
    }

private:
    std::vector<PartsCollectorPtr> collectors;
};

}
