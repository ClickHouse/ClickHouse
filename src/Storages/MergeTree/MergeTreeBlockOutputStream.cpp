#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/PartLog.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/CollapsingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/SummingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/AggregatingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/VersionedCollapsingAlgorithm.h>
#include <Processors/Merges/Algorithms/GraphiteRollupSortedAlgorithm.h>
#include <Interpreters/sortBlock.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Block MergeTreeBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}

Block MergeTreeBlockOutputStream::mergeBlock(const Block & block)
{
    /// Get the information needed for merging algorithms
    size_t block_size = block.rows();
    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(getHeader().getPositionByName(sort_columns[i]), 1, 1);

    auto get_merging_algorithm = [&]() -> std::shared_ptr<IMergingAlgorithm>
    {
        switch (storage.merging_params.mode)
        {
            /// There is nothing to merge in single block in ordinary MergeTree
            case MergeTreeData::MergingParams::Ordinary:
                return nullptr;
            case MergeTreeData::MergingParams::Replacing:
                return std::make_shared<ReplacingSortedAlgorithm>(
                    getHeader(), 1, sort_description, storage.merging_params.version_column, block_size);
            case MergeTreeData::MergingParams::Collapsing:
                return std::make_shared<CollapsingSortedAlgorithm>(
                    getHeader(), 1, sort_description, storage.merging_params.sign_column,
                    false,  block_size, &Poco::Logger::get("MergeTreeBlockOutputStream"));
            case MergeTreeData::MergingParams::Summing:
                return std::make_shared<SummingSortedAlgorithm>(
                    getHeader(), 1, sort_description, storage.merging_params.columns_to_sum,
                    metadata_snapshot->getPartitionKey().column_names, block_size);
            case MergeTreeData::MergingParams::Aggregating:
                return std::make_shared<AggregatingSortedAlgorithm>(getHeader(), 1, sort_description, block_size);
            case MergeTreeData::MergingParams::VersionedCollapsing:
                return std::make_shared<VersionedCollapsingAlgorithm>(
                    getHeader(), 1, sort_description, storage.merging_params.sign_column, block_size);
            case MergeTreeData::MergingParams::Graphite:
                return std::make_shared<GraphiteRollupSortedAlgorithm>(
                    getHeader(), 1, sort_description, block_size, storage.merging_params.graphite_params, time(nullptr));
        }

        __builtin_unreachable();
    };

    auto merging_algorithm = get_merging_algorithm();
    if (!merging_algorithm)
        return block;

    /// Merging algorithms works with inputs containing sorted chunks, so we need to get a sorted permutation
    /// of the block, convert the block to a chunk and construct an input from it
    IColumn::Permutation permutation;
    stableGetPermutation(block, sort_description, permutation);

    Chunk chunk(block.getColumns(), block_size);

    IMergingAlgorithm::Input input;
    input.set(std::move(chunk));
    input.permutation = &permutation;

    IMergingAlgorithm::Inputs inputs;
    inputs.push_back(std::move(input));
    merging_algorithm->initialize(std::move(inputs));

    IMergingAlgorithm::Status status = merging_algorithm->merge();
    while (!status.is_finished)
        status = merging_algorithm->merge();

    return block.cloneWithColumns(status.chunk.getColumns());
}


void MergeTreeBlockOutputStream::write(const Block & block)
{
    storage.delayInsertOrThrowIfNeeded();

    auto settings = context.getSettings();

    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot);
    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;

        if (settings.optimize_on_insert)
            current_block.block = mergeBlock(current_block.block);

        MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, metadata_snapshot);
        storage.renameTempPartAndAdd(part, &storage.increment);

        PartLog::addNewPart(storage.global_context, part, watch.elapsed());

        /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
        storage.background_executor.triggerTask();
    }
}

}
