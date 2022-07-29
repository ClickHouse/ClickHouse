#include "MergeTreeDataPartCloner.h"
#include <Interpreters/MergeTreeTransaction.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

    MergeTreeDataPartCloner::MergeTreeDataPartCloner(
        const MergeTreeData & merge_tree_data_,
        const DataPartPtr & src_part_,
        const StorageMetadataPtr & metadata_snapshot_,
        const MergeTreePartInfo & dst_part_info_,
        const String & tmp_part_prefix_,
        const MergeTreeTransactionPtr & txn_,
        bool require_part_metadata_,
        MergeTreeData::HardlinkedFiles * hardlinked_files_,
        bool copy_instead_of_hardlink_
    )
    : merge_tree_data(merge_tree_data_), src_part(src_part_), metadata_snapshot(metadata_snapshot_),
        dst_part_info(dst_part_info_), tmp_part_prefix(tmp_part_prefix_),
        txn(txn_), require_part_metadata(require_part_metadata_),
        hardlinked_files(hardlinked_files_), copy_instead_of_hardlink(copy_instead_of_hardlink_)
    {}

    MergeTreeDataPartCloner::MutableDataPartPtr MergeTreeDataPartCloner::clone()
    {
        if (!does_storage_policy_allow_same_disk())
            throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Could not clone and load part {} because disk does not belong to storage policy",
                    quoteString(src_part->data_part_storage->getFullPath()));

        assert(!tmp_part_prefix.empty());

        const auto destination_part = clone_source_part();

        if (!copy_instead_of_hardlink && hardlinked_files)
        {
            // think of a name for this method
            handle_hard_linked_parameter_files();
        }

        return finalize_part(destination_part);
    }

    DataPartStoragePtr MergeTreeDataPartCloner::flush_part_storage_to_disk_if_in_memory() const
    {
        if (auto src_part_in_memory = asInMemoryPart(src_part))
        {
            auto flushed_part_path = src_part_in_memory->getRelativePathForPrefix(tmp_part_prefix);
            return src_part_in_memory->flushToDisk(flushed_part_path, metadata_snapshot);
        }

        return src_part->data_part_storage;
    }

    bool MergeTreeDataPartCloner::does_storage_policy_allow_same_disk() const
    {
        for (const DiskPtr & disk : merge_tree_data.getStoragePolicy()->getDisks())
        {
            if (disk->getName() == src_part->data_part_storage->getDiskName())
            {
                return true;
            }
        }
        return false;
    }

    void MergeTreeDataPartCloner::reserve_space_on_disk() const
    {
        src_part->data_part_storage->reserve(src_part->getBytesOnDisk());
    }

    std::shared_ptr<IDataPartStorage> MergeTreeDataPartCloner::hardlink_all_files(
            const DataPartStoragePtr & storage,
            const String & path
    ) const
    {
        return storage->freeze(
                merge_tree_data.getRelativeDataPath(),
                path,
                false /* make_source_readonly */,
                {},
                false /* copy_instead_of_hardlinks */
        );
    }

    MergeTreeDataPartCloner::MutableDataPartPtr MergeTreeDataPartCloner::clone_source_part() const
    {
        const String dst_part_name = src_part->getNewName(dst_part_info);

        const String tmp_dst_part_name = tmp_part_prefix + dst_part_name;

        reserve_space_on_disk();

        auto src_part_storage = flush_part_storage_to_disk_if_in_memory();

        auto dst_part_storage = hardlink_all_files(src_part_storage, tmp_dst_part_name);

        return merge_tree_data.createPart(dst_part_name, dst_part_info, dst_part_storage);
    }

    void MergeTreeDataPartCloner::handle_hard_linked_parameter_files() const
    {
        hardlinked_files->source_part_name = src_part->name;
        hardlinked_files->source_table_shared_id = src_part->storage.getTableSharedID();

        for (auto it = src_part->data_part_storage->iterate(); it->isValid(); it->next())
        {
            if (it->name() != IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME && it->name() != IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME)
                hardlinked_files->hardlinks_from_source_part.insert(it->name());
        }
    }

    MergeTreeDataPartCloner::MutableDataPartPtr MergeTreeDataPartCloner::finalize_part(const MutableDataPartPtr & dst_part) const
    {
        /// We should write version metadata on part creation to distinguish it from parts that were created without transaction.
        TransactionID tid = txn ? txn->tid : Tx::PrehistoricTID;
        dst_part->version.setCreationTID(tid, nullptr);
        dst_part->storeVersionMetadata();

        dst_part->is_temp = true;

        dst_part->loadColumnsChecksumsIndexes(require_part_metadata, true);

        dst_part->modification_time = dst_part->data_part_storage->getLastModified().epochTime();

        return dst_part;
    }

}
