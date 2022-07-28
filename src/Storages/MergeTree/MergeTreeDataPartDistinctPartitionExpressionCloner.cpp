#include "MergeTreeDataPartDistinctPartitionExpressionCloner.h"
#include <Common/escapeForFileName.h>
#include <Interpreters/MergeTreeTransaction.h>

namespace DB
{

    MergeTreeDataPartDistinctPartitionExpressionCloner::MergeTreeDataPartDistinctPartitionExpressionCloner(
        const MergeTreeData & merge_tree_data_,
        const DataPartPtr & src_part_,
        const MergeTreePartInfo & dst_part_info_,
        const String & tmp_part_prefix_,
        const MergeTreeTransactionPtr & txn_,
        const MergeTreePartition & new_partition_,
        const IMergeTreeDataPart::MinMaxIndex & new_min_max_index_
    )
    : MergeTreeDataPartCloner(merge_tree_data_, src_part_,merge_tree_data_.getInMemoryMetadataPtr(),
                              dst_part_info_, tmp_part_prefix_, txn_, false, {}, false),
        new_partition(new_partition_), new_min_max_index(new_min_max_index_)
    {}

    void MergeTreeDataPartDistinctPartitionExpressionCloner::delete_min_max_files(
        const DataPartStorageBuilderPtr & storage_builder
    ) const
    {
        for (const auto & column : src_part->getColumns())
        {
            auto file = "minmax_" + escapeForFileName(column.name) + ".idx";
            storage_builder->removeFile(file);
        }
    }

    void MergeTreeDataPartDistinctPartitionExpressionCloner::update_min_max_files(
        const MutableDataPartPtr & dst_part,
        const DataPartStorageBuilderPtr & storage_builder
    ) const
    {
        delete_min_max_files(storage_builder);

        [[maybe_unused]] auto written_files = dst_part->minmax_idx->store(merge_tree_data, storage_builder, dst_part->checksums);
    }

    void MergeTreeDataPartDistinctPartitionExpressionCloner::update_partition_file(
        const MergeTreePartition & partition,
        const MutableDataPartPtr & dst_part,
        const DataPartStorageBuilderPtr & storage_builder
    ) const
    {
        storage_builder->removeFile("partition.dat");

        // Leverage already implemented MergeTreePartition::store to create & store partition.dat.
        // Checksum is re-calculated later.
        auto partition_store_write_buffer = partition.store(merge_tree_data, storage_builder, dst_part->checksums);

        partition_store_write_buffer->finalize();
    }

    void MergeTreeDataPartDistinctPartitionExpressionCloner::update_new_part_files(const MutableDataPartPtr & dst_part) const
    {
        auto data_part_storage_builder = dst_part->data_part_storage->getBuilder();

        dst_part->minmax_idx->replace(new_min_max_index);

        update_partition_file(new_partition, dst_part, data_part_storage_builder);

        update_min_max_files(dst_part, data_part_storage_builder);

        // MergeTreeDataPartCloner::finalize_part calls IMergeTreeDataPart::loadColumnsChecksumsIndexes, which will re-create
        // the checksum file if it doesn't exist. Relying on that is cumbersome, but this refactoring is simply a code extraction
        // with small improvements. It can be further improved in the future.
        data_part_storage_builder->removeFile("checksums.txt");
    }

    MergeTreeDataPartCloner::MutableDataPartPtr MergeTreeDataPartDistinctPartitionExpressionCloner::finalize_part(const MutableDataPartPtr & dst_part) const
    {
        update_new_part_files(dst_part);

        return MergeTreeDataPartCloner::finalize_part(dst_part);
    }

}
