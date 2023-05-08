#include <Storages/MergeTree/MergeTreePartitionGlobalMinMaxIdxCalculator.h>

namespace DB
{

std::pair<Field, Field> MergeTreePartitionGlobalMinMaxIdxCalculator::extractMinMaxFromBlock(const Block & block)
{
    Field min_idx;
    Field max_idx;

    block.getByPosition(0).column->get(0, min_idx);
    block.getByPosition(0).column->get(1, max_idx);

    return std::make_pair(min_idx, max_idx);
}

std::pair<Field, Field> MergeTreePartitionGlobalMinMaxIdxCalculator::calculate(const MergeTreeData & storage,
                                                                               const DataPartsVector & parts)
{
    Field min_idx;
    Field max_idx;

    for (const auto & part : parts)
    {
        auto metadata_manager = std::make_shared<PartMetadataManagerOrdinary>(part.get());

        DataPart::MinMaxIndex min_max_index;

        min_max_index.load(storage, metadata_manager);

        Block block_with_min_max_partition_ids = min_max_index.getBlock(storage);

        auto [min_tmp, max_tmp] = extractMinMaxFromBlock(block_with_min_max_partition_ids);

        if (min_idx.isNull() || min_tmp < min_idx)
        {
            min_idx = min_tmp;
        }

        if (max_idx.isNull() || max_tmp > max_idx)
        {
            max_idx = max_tmp;
        }
    }

    return std::make_pair(min_idx, max_idx);
}

}
