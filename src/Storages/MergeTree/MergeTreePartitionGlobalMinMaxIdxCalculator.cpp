#include <Storages/MergeTree/MergeTreePartitionGlobalMinMaxIdxCalculator.h>

namespace DB
{


namespace
{
    Block loadMinMaxIndexesForPart(
        const MergeTreeData & storage,
        const MergeTreeData::DataPartPtr & part
    )
    {
        auto metadata_manager = std::make_shared<PartMetadataManagerOrdinary>(part.get());

        auto min_max_index = MergeTreeData::DataPart::MinMaxIndex();

        min_max_index.load(storage, metadata_manager);

        return min_max_index.getBlock(storage);
    }

    void updateGlobalMinMaxBlock(
        Block & global_min_max_block,
        const Block & local_min_max_block,
        const Names & columns_of_interest
    )
    {
        if (global_min_max_block.columns() == 0)
        {
            global_min_max_block = local_min_max_block.cloneWithOnlyColumns(columns_of_interest);
            return;
        }

        assert(local_min_max_block.columns() == global_min_max_block.columns());

        for (size_t i = 0; i < global_min_max_block.columns(); ++i)
        {
            const auto & localColumn = local_min_max_block.getByPosition(i);
            auto & globalColumn = global_min_max_block.getByPosition(i);

            Field local_min_idx;
            Field local_max_idx;

            Field global_min_idx;
            Field global_max_idx;

            localColumn.column->get(0, local_min_idx);
            localColumn.column->get(1, local_max_idx);

            globalColumn.column->get(0, global_min_idx);
            globalColumn.column->get(1, global_max_idx);

            auto new_column = globalColumn.type->createColumn();

            new_column->insert(std::min(global_min_idx, local_min_idx));
            new_column->insert(std::max(global_max_idx, local_max_idx));

            globalColumn.column = std::move(new_column);
        }
    }

}

Block MergeTreePartitionGlobalMinMaxIdxCalculator::calculate(
    const MergeTreeData & storage,
    const DataPartsVector & parts,
    const Names & columns_of_interest
)
{
    Block global_min_max_indexes;

    for (const auto & part : parts)
    {
        auto min_max_indexes_block = loadMinMaxIndexesForPart(storage, part);

        updateGlobalMinMaxBlock(global_min_max_indexes, loadMinMaxIndexesForPart(storage, part), columns_of_interest);
    }

    return global_min_max_indexes;
}

}
