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

    PartsRanges collectPartsToUse(const MergeTreeTransactionPtr & tx, const PartitionIdsHint * partitions_hint) const override
    {
        PartsRanges ranges;

        for (const auto & collector : collectors)
        {
            auto collected_ranges = collector->collectPartsToUse(tx, partitions_hint);
            insertAtEnd(ranges, std::move(collected_ranges));
        }

        /// TODO (michicosun): Add sanity check for intersecting parts here

        return ranges;
    }

private:
    std::vector<PartsCollectorPtr> collectors;
};

}
