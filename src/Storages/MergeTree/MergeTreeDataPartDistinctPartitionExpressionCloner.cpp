#include "MergeTreeDataPartDistinctPartitionExpressionCloner.h"
#include <Common/escapeForFileName.h>
#include <Interpreters/MergeTreeTransaction.h>

namespace DB
{

MergeTreeDataPartDistinctPartitionExpressionCloner::MergeTreeDataPartDistinctPartitionExpressionCloner(
    MergeTreeData * merge_tree_data_,
    const DataPartPtr & src_part_,
    const MergeTreePartInfo & dst_part_info_,
    const String & tmp_part_prefix_,
    const MergeTreeTransactionPtr & txn_,
    const MergeTreePartition & new_partition_,
    const IMergeTreeDataPart::MinMaxIndex & new_min_max_index_,
    bool sync_new_files_
)
: MergeTreeDataPartCloner(merge_tree_data_, src_part_, merge_tree_data_->getInMemoryMetadataPtr(),
                          dst_part_info_, tmp_part_prefix_, txn_, false, {}, false, {}),
    new_partition(new_partition_), new_min_max_index(new_min_max_index_), sync_new_files(sync_new_files_)
{}

void MergeTreeDataPartDistinctPartitionExpressionCloner::deleteMinMaxFiles(
    const DataPartStorageBuilderPtr & storage_builder
) const
{
    for (const auto & column : src_part->getColumns())
    {
        auto file = "minmax_" + escapeForFileName(column.name) + ".idx";
        storage_builder->removeFile(file);
    }
}

MergeTreeDataPartDistinctPartitionExpressionCloner::WrittenFiles MergeTreeDataPartDistinctPartitionExpressionCloner::updateMinMaxFiles(
    const MutableDataPartPtr & dst_part,
    const DataPartStorageBuilderPtr & storage_builder
) const
{
    deleteMinMaxFiles(storage_builder);

    return dst_part->minmax_idx->store(*merge_tree_data, storage_builder, dst_part->checksums);
}

MergeTreeDataPartDistinctPartitionExpressionCloner::WrittenFile MergeTreeDataPartDistinctPartitionExpressionCloner::updatePartitionFile(
    const MergeTreePartition & partition,
    const MutableDataPartPtr & dst_part,
    const DataPartStorageBuilderPtr & storage_builder
) const
{
    storage_builder->removeFile("partition.dat");
    // Leverage already implemented MergeTreePartition::store to create & store partition.dat.
    // Checksum is re-calculated later.
    return partition.store(*merge_tree_data, storage_builder, dst_part->checksums);
}

void MergeTreeDataPartDistinctPartitionExpressionCloner::updateNewPartFiles(const MutableDataPartPtr & dst_part) const
{
    auto data_part_storage_builder = dst_part->data_part_storage->getBuilder();

    *dst_part->minmax_idx = new_min_max_index;

    auto partition_file = updatePartitionFile(new_partition, dst_part, data_part_storage_builder);

    auto min_max_files = updateMinMaxFiles(dst_part, data_part_storage_builder);

    WrittenFiles written_files;

    written_files.emplace_back(std::move(partition_file));

    written_files.insert(written_files.end(), std::make_move_iterator(min_max_files.begin()), std::make_move_iterator(min_max_files.end()));

    finalizeNewFiles(written_files);

    // MergeTreeDataPartCloner::finalize_part calls IMergeTreeDataPart::loadColumnsChecksumsIndexes, which will re-create
    // the checksum file if it doesn't exist. Relying on that is cumbersome, but this refactoring is simply a code extraction
    // with small improvements. It can be further improved in the future.
    data_part_storage_builder->removeFile("checksums.txt");
}

void MergeTreeDataPartDistinctPartitionExpressionCloner::finalizeNewFiles(const WrittenFiles & files) const
{
    for (const auto & file : files)
    {
        file->finalize();
        if (sync_new_files)
        {
            file->sync();
        }
    }
}

MergeTreeDataPartCloner::MutableDataPartPtr MergeTreeDataPartDistinctPartitionExpressionCloner::finalizePart(const MutableDataPartPtr & dst_part) const
{
    updateNewPartFiles(dst_part);

    return MergeTreeDataPartCloner::finalizePart(dst_part);
}

}
