#include <Storages/MergeTree/MergeTreePartitionGlobalMinMaxIdxCalculator.h>

namespace DB
{

std::vector<std::pair<Field, Field>> MergeTreePartitionGlobalMinMaxIdxCalculator::extractMinMaxIndexesFromBlock(
    const Block & block,
    const std::vector<std::size_t> & column_indexes
)
{
    std::vector<std::pair<Field, Field>> min_max_indexes;

    for (auto index : column_indexes)
    {
        Field min_idx;
        Field max_idx;

        block.getByPosition(index).column->get(0, min_idx);
        block.getByPosition(index).column->get(1, max_idx);

        min_max_indexes.emplace_back(min_idx, max_idx);
    }

    return min_max_indexes;
}

std::vector<std::pair<Field, Field>> MergeTreePartitionGlobalMinMaxIdxCalculator::calculate(
    const MergeTreeData & storage,
    const DataPartsVector & parts,
    const std::vector<std::size_t> & column_indexes
)
{
    std::vector<std::pair<Field, Field>> global_min_max_indexes(column_indexes.size());

    for (const auto & part : parts)
    {
        auto metadata_manager = std::make_shared<PartMetadataManagerOrdinary>(part.get());

        DataPart::MinMaxIndex min_max_index;

        min_max_index.load(storage, metadata_manager);

        Block block_with_min_max_partition_ids = min_max_index.getBlock(storage);

        auto local_min_max_indexes = extractMinMaxIndexesFromBlock(block_with_min_max_partition_ids, column_indexes);

        for (auto i = 0u; i < local_min_max_indexes.size(); i++)
        {
            const auto & [local_min_idx, local_max_idx] = local_min_max_indexes[i];

            const auto & [global_min_idx, global_max_idx] = global_min_max_indexes[i];

            if (global_min_idx.isNull() || local_min_idx < global_min_idx)
            {
                global_min_max_indexes[i].first = local_min_idx;
            }

            if (global_max_idx.isNull() || local_max_idx > global_max_idx)
            {
                global_min_max_indexes[i].second = local_max_idx;
            }
        }
    }

    return global_min_max_indexes;
}

}
