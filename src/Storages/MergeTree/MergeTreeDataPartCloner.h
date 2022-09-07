#pragma once

#include "MergeTreeData.h"

namespace DB
{
    /*
     * Clones a source partition into a destination partition. This class was extracted from MergeTreeData::cloneAndLoadDataPartOnSameDisk
     * as an attempt to make it a bit more readable and allow part of the code to be re-used. Reference method can be found in:
     * https://github.com/ClickHouse/ClickHouse/blob/ee515b8862a9be0c940ffdef0a56e590b5facdb2/src/Storages/MergeTree/MergeTreeData.cpp#L5948.
     * */
    class MergeTreeDataPartCloner
    {
    public:
        using DataPart = IMergeTreeDataPart;
        using MutableDataPartPtr = std::shared_ptr<DataPart>;
        using DataPartPtr = std::shared_ptr<const DataPart>;

        MergeTreeDataPartCloner(
            MergeTreeData * merge_tree_data,
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

        std::pair<MutableDataPartPtr, scope_guard> clone();

    protected:
        MergeTreeData * merge_tree_data;
        const DataPartPtr & src_part;

        virtual MergeTreeData::MutableDataPartPtr finalizePart(const MutableDataPartPtr & dst_part) const;

    private:
        const StorageMetadataPtr & metadata_snapshot;
        const MergeTreePartInfo & dst_part_info;
        const String & tmp_part_prefix;
        const MergeTreeTransactionPtr & txn;
        bool require_part_metadata;
        MergeTreeData::HardlinkedFiles * hardlinked_files;
        bool copy_instead_of_hardlink;

        /// Check that the storage policy contains the disk where the src_part is located.
        bool doesStoragePolicyAllowSameDisk() const;

        void reserveSpaceOnDisk() const;

        std::shared_ptr<IDataPartStorage> hardlinkAllFiles(const DataPartStoragePtr & storage, const String & path) const;

        /// If source part is in memory, flush it to disk and clone it already in on-disk format
        DataPartStoragePtr flushPartStorageToDiskIfInMemory() const;

        std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> cloneSourcePart() const;

        void handleHardLinkedParameterFiles() const;

    };
}
