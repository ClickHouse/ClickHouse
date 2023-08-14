#include "MergeTreeDataPartDistinctPartitionExpressionCloner.h"
#include <Common/escapeForFileName.h>
#include <Interpreters/MergeTreeTransaction.h>

namespace DB
{

MergeTreeDataPartDistinctPartitionExpressionCloner::MergeTreeDataPartDistinctPartitionExpressionCloner(
    MergeTreeData * merge_tree_data_,
    const DataPartPtr & src_part_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreePartInfo & dst_part_info_,
    const String & tmp_part_prefix_,
    const MergeTreePartition & new_partition_,
    const IMergeTreeDataPart::MinMaxIndex & new_min_max_index_,
    bool sync_new_files_,
    const IDataPartStorage::ClonePartParams & params_
)
: MergeTreeDataPartCloner(merge_tree_data_, src_part_, metadata_snapshot_,
                          dst_part_info_, tmp_part_prefix_, false, params_),
    new_partition(new_partition_), new_min_max_index(new_min_max_index_), sync_new_files(sync_new_files_)
{}

void MergeTreeDataPartDistinctPartitionExpressionCloner::deleteMinMaxFiles(
    IDataPartStorage & storage
) const
{
    for (const auto & column_name : merge_tree_data->getMinMaxColumnsNames(metadata_snapshot->partition_key))
    {
        auto file = "minmax_" + escapeForFileName(column_name) + ".idx";
        storage.removeFile(file);
    }
}

MergeTreeDataPartDistinctPartitionExpressionCloner::WrittenFiles MergeTreeDataPartDistinctPartitionExpressionCloner::updateMinMaxFiles(
    const MutableDataPartPtr & dst_part,
    IDataPartStorage & storage
) const
{
    deleteMinMaxFiles(storage);

    return dst_part->minmax_idx->store(*merge_tree_data, storage, dst_part->checksums);
}

MergeTreeDataPartDistinctPartitionExpressionCloner::WrittenFile MergeTreeDataPartDistinctPartitionExpressionCloner::updatePartitionFile(
    const MergeTreePartition & partition,
    const MutableDataPartPtr & dst_part,
    IDataPartStorage & storage
) const
{
    storage.removeFile("partition.dat");
    // Leverage already implemented MergeTreePartition::store to create & store partition.dat.
    // Checksum is re-calculated later.
    return partition.store(*merge_tree_data, storage, dst_part->checksums);
}

void MergeTreeDataPartDistinctPartitionExpressionCloner::updateNewPartFiles(const MutableDataPartPtr & dst_part) const
{
    auto & storage = dst_part->getDataPartStorage();

    *dst_part->minmax_idx = new_min_max_index;

    auto partition_file = updatePartitionFile(new_partition, dst_part, storage);

    auto min_max_files = updateMinMaxFiles(dst_part, storage);

    WrittenFiles written_files;

    written_files.emplace_back(std::move(partition_file));

    written_files.insert(written_files.end(), std::make_move_iterator(min_max_files.begin()), std::make_move_iterator(min_max_files.end()));

    finalizeNewFiles(written_files);

    // MergeTreeDataPartCloner::finalize_part calls IMergeTreeDataPart::loadColumnsChecksumsIndexes, which will re-create
    // the checksum file if it doesn't exist. Relying on that is cumbersome, but this refactoring is simply a code extraction
    // with small improvements. It can be further improved in the future.
    storage.removeFile("checksums.txt");
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
