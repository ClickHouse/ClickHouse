#pragma once

#include "MergeTreeDataPartCloner.h"

namespace DB
{
class MergeTreeTransaction;
using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

/*
 * Clones a source partition into a destination partition by hard linking all files except partition.dat,
 * minmax_<partition_expression_column>.idx and checksums.txt. These files are re-calculated and store separately
 * in the destination partition directory.
 * */
class MergeTreeDataPartDistinctPartitionExpressionCloner : public MergeTreeDataPartCloner
{
    using WrittenFile = std::unique_ptr<WriteBufferFromFileBase>;
    using WrittenFiles = std::vector<WrittenFile>;
public:
    MergeTreeDataPartDistinctPartitionExpressionCloner(
        MergeTreeData * merge_tree_data,
        const DataPartPtr & src_part,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreePartInfo & dst_part_info,
        const String & tmp_part_prefix,
        const MergeTreePartition & new_partition,
        const IMergeTreeDataPart::MinMaxIndex & new_min_max_index,
        bool sync_new_files,
        const IDataPartStorage::ClonePartParams & params
    );

private:
    const MergeTreePartition & new_partition;
    const IMergeTreeDataPart::MinMaxIndex & new_min_max_index;
    const bool sync_new_files;

    void deleteMinMaxFiles(IDataPartStorage & storage) const;

    WrittenFiles updateMinMaxFiles(
        const MutableDataPartPtr & dst_part,
        IDataPartStorage & storage
    ) const;

    WrittenFile updatePartitionFile(
        const MergeTreePartition & new_partition,
        const MutableDataPartPtr & dst_part,
        IDataPartStorage & storage
    ) const;

    /// Re-writes partition.dat and minmax_<fields>.idx. Also deletes checksums.txt
    void updateNewPartFiles(const MutableDataPartPtr & dst_part) const;

    void finalizeNewFiles(const WrittenFiles & min_max_files) const;

    MergeTreeDataPartCloner::MutableDataPartPtr finalizePart(const MutableDataPartPtr & dst_part) const override;

};
}
