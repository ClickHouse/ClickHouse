#include <Storages/MergeTree/MergeTreePartitionGlobalMinMaxIdxCalculator.h>

namespace DB
{

std::vector<std::pair<Field, Field>> MergeTreePartitionGlobalMinMaxIdxCalculator::extractMinMaxIndexesFromBlock(
    const Block & block_with_min_max_idx
)
{
    std::vector<std::pair<Field, Field>> min_max_indexes;

    for (auto i = 0u; i < block_with_min_max_idx.columns(); i++)
    {
        Field min_idx;
        Field max_idx;

        block_with_min_max_idx.getByPosition(i).column->get(0, min_idx);
        block_with_min_max_idx.getByPosition(i).column->get(1, max_idx);

        min_max_indexes.emplace_back(std::move(min_idx), std::move(max_idx));
    }

    return min_max_indexes;
}

std::vector<std::pair<Field, Field>> MergeTreePartitionGlobalMinMaxIdxCalculator::calculateMinMaxIndexesForPart(
    const MergeTreeData & storage,
    const DataPartPtr & part
)
{
    auto metadata_manager = std::make_shared<PartMetadataManagerOrdinary>(part.get());

    auto min_max_index = DataPart::MinMaxIndex();

    min_max_index.load(storage, metadata_manager);

    auto block_with_min_max_partition_ids = min_max_index.getBlock(storage);

    return extractMinMaxIndexesFromBlock(block_with_min_max_partition_ids);
}

void MergeTreePartitionGlobalMinMaxIdxCalculator::updateGlobalMinMaxIndexes(
    std::vector<std::pair<Field, Field>> & global_min_max_indexes,
    const std::vector<std::pair<Field, Field>> & local_min_max_indexes
)
{
    if (global_min_max_indexes.empty())
    {
        global_min_max_indexes = local_min_max_indexes;
        return;
    }

    for (auto i = 0u; i < local_min_max_indexes.size(); i++)
    {
        const auto & [local_min_idx, local_max_idx] = local_min_max_indexes[i];

        global_min_max_indexes[i] = {
            std::min(global_min_max_indexes[i].first, local_min_idx),
            std::max(global_min_max_indexes[i].second, local_max_idx)
        };
    }
}

std::vector<std::pair<Field, Field>> MergeTreePartitionGlobalMinMaxIdxCalculator::calculate(
    const MergeTreeData & storage,
    const DataPartsVector & parts
)
{
    std::vector<std::pair<Field, Field>> global_min_max_indexes;

    for (const auto & part : parts)
    {
        updateGlobalMinMaxIndexes(global_min_max_indexes, calculateMinMaxIndexesForPart(storage, part));
    }

    return global_min_max_indexes;
}

}
