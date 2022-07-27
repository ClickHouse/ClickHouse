#pragma once

#include "MergeTreeData.h"

namespace DB
{
    struct MergeTreeDataPartCloner
    {
    public:
        using DataPart = IMergeTreeDataPart;
        using MutableDataPartPtr = std::shared_ptr<DataPart>;
        using DataPartPtr = std::shared_ptr<const DataPart>;

        MergeTreeDataPartCloner(
            const MergeTreeData & merge_tree_data,
            const DataPartPtr & src_part,
            const StorageMetadataPtr & metadata_snapshot,
            const MergeTreePartInfo & dst_part_info,
            const String & tmp_part_prefix,
            const MergeTreeTransactionPtr & txn,
            bool require_part_metadata,
            MergeTreeData::HardlinkedFiles * hardlinked_files,
            bool copy_instead_of_hardlink
        );

        virtual ~MergeTreeDataPartCloner() = default;

        MutableDataPartPtr clone();

    protected:
        const MergeTreeData & merge_tree_data;
        const DataPartPtr & src_part;

        virtual MutableDataPartPtr finalize_part(const MutableDataPartPtr & dst_part) const;

    private:
        const StorageMetadataPtr & metadata_snapshot;
        const MergeTreePartInfo & dst_part_info;
        const String & tmp_part_prefix;
        const MergeTreeTransactionPtr & txn;
        bool require_part_metadata;
        MergeTreeData::HardlinkedFiles * hardlinked_files;
        bool copy_instead_of_hardlink;

        /// Check that the storage policy contains the disk where the src_part is located.
        bool does_storage_policy_allow_same_disk() const;

        void reserve_space_on_disk() const;

        std::shared_ptr<IDataPartStorage> hardlink_all_files(const DataPartStoragePtr & storage, const String & path) const;

        /// If source part is in memory, flush it to disk and clone it already in on-disk format
        DataPartStoragePtr flush_part_storage_to_disk_if_in_memory() const;

        MutableDataPartPtr clone_source_part() const;

        void handle_hard_linked_parameter_files() const;

    };
}
