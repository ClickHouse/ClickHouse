#include <Storages/MergeTree/MergeTreePartitionGlobalMinMaxIdxCalculator.h>

namespace DB
{


namespace
{

std::vector<Range> extractMinMaxIndexesFromBlock(
        const Block & block_with_min_max_idx
    )
    {
        std::vector<Range> min_max_indexes;

        for (auto i = 0u; i < block_with_min_max_idx.columns(); i++)
        {
            Field min_idx;
            Field max_idx;

            block_with_min_max_idx.getByPosition(i).column->get(0, min_idx);
            block_with_min_max_idx.getByPosition(i).column->get(1, max_idx);

            min_max_indexes.emplace_back(Range(std::move(min_idx), true, std::move(max_idx), true));
        }

        return min_max_indexes;
    }

    std::vector<Range> calculateMinMaxIndexesForPart(
        const MergeTreeData & storage,
        const MergeTreeData::DataPartPtr & part
    )
    {
        auto metadata_manager = std::make_shared<PartMetadataManagerOrdinary>(part.get());

        auto min_max_index = MergeTreeData::DataPart::MinMaxIndex();

        min_max_index.load(storage, metadata_manager);

        auto block_with_min_max_partition_ids = min_max_index.getBlock(storage);

        return extractMinMaxIndexesFromBlock(block_with_min_max_partition_ids);
    }

    void updateGlobalMinMaxIndexes(
        std::vector<Range> & global_min_max_indexes,
        const std::vector<Range> & local_min_max_indexes
    )
    {
        if (global_min_max_indexes.empty())
        {
            global_min_max_indexes = local_min_max_indexes;
            return;
        }

        assert(local_min_max_indexes.size() == global_min_max_indexes.size());

        for (auto i = 0u; i < local_min_max_indexes.size(); i++)
        {
            const auto & local_range = local_min_max_indexes[i];

            global_min_max_indexes[i] = {
                std::min(global_min_max_indexes[i].left, local_range.left),
                true,
                std::max(global_min_max_indexes[i].right, local_range.right),
                true
            };
        }
    }

}

std::vector<Range> MergeTreePartitionGlobalMinMaxIdxCalculator::calculate(
    const MergeTreeData & storage,
    const DataPartsVector & parts
)
{
    std::vector<Range> global_min_max_indexes;

    for (const auto & part : parts)
    {
        updateGlobalMinMaxIndexes(global_min_max_indexes, calculateMinMaxIndexesForPart(storage, part));
    }

    return global_min_max_indexes;
}

}
