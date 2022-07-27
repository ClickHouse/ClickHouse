#pragma once

#include "MergeTreeDataPartCloner.h"

namespace DB
{
    class MergeTreeTransaction;
    using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

    class MergeTreeDataPartDistinctPartitionExpressionCloner : public MergeTreeDataPartCloner
    {

    public:
        MergeTreeDataPartDistinctPartitionExpressionCloner(
            const MergeTreeData & merge_tree_data,
            const DataPartPtr & src_part,
            const MergeTreePartInfo & dst_part_info,
            const String & tmp_part_prefix,
            const MergeTreeTransactionPtr & txn,
            const MergeTreePartition & new_partition,
            const IMergeTreeDataPart::MinMaxIndex & new_min_max_index
        );

    private:
        const MergeTreePartition & new_partition;
        const IMergeTreeDataPart::MinMaxIndex & new_min_max_index;

        void delete_min_max_files(const DataPartStorageBuilderPtr & storage_builder) const;

        void update_min_max_files(
            const MutableDataPartPtr & dst_part,
            const DataPartStorageBuilderPtr & storage_builder
        ) const;

        void update_partition_file(
            const MergeTreePartition & new_partition,
            const MutableDataPartPtr & dst_part,
            const DataPartStorageBuilderPtr & storage_builder
        ) const;

        void update_new_part_files(const MutableDataPartPtr & dst_part) const;

        MutableDataPartPtr finalize_part(const MutableDataPartPtr & dst_part) const override;

        void handle_hard_linked_parameter_files(MergeTreeData::HardlinkedFiles * hardlinked_files) const;

    };
}
