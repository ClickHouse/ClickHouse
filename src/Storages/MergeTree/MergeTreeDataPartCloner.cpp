#include "MergeTreeDataPartCloner.h"
#include <Interpreters/MergeTreeTransaction.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

MergeTreeDataPartCloner::MergeTreeDataPartCloner(
    MergeTreeData * merge_tree_data_,
    const DataPartPtr & src_part_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreePartInfo & dst_part_info_,
    const String & tmp_part_prefix_,
    const MergeTreeTransactionPtr & txn_,
    bool require_part_metadata_,
    MergeTreeData::HardlinkedFiles * hardlinked_files_,
    bool copy_instead_of_hardlink_,
    const NameSet & files_to_copy_instead_of_hardlinks_
)
: merge_tree_data(merge_tree_data_), src_part(src_part_), metadata_snapshot(metadata_snapshot_),
    dst_part_info(dst_part_info_), tmp_part_prefix(tmp_part_prefix_),
    txn(txn_), require_part_metadata(require_part_metadata_),
    hardlinked_files(hardlinked_files_), copy_instead_of_hardlink(copy_instead_of_hardlink_),
    files_to_copy_instead_of_hardlinks(files_to_copy_instead_of_hardlinks_)
{}

std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> MergeTreeDataPartCloner::clone()
{
    if (!doesStoragePolicyAllowSameDisk())
        throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Could not clone and load part {} because disk does not belong to storage policy",
                quoteString(src_part->data_part_storage->getFullPath()));

    assert(!tmp_part_prefix.empty());

    auto [destination_part, temporary_directory_lock] = cloneSourcePart();

    if (!copy_instead_of_hardlink && hardlinked_files)
    {
        // think of a name for this method
        handleHardLinkedParameterFiles();
    }

    return std::make_pair(finalizePart(destination_part), std::move(temporary_directory_lock));
}

DataPartStoragePtr MergeTreeDataPartCloner::flushPartStorageToDiskIfInMemory() const
{
    if (auto src_part_in_memory = asInMemoryPart(src_part))
    {
        auto flushed_part_path = src_part_in_memory->getRelativePathForPrefix(tmp_part_prefix);
        return src_part_in_memory->flushToDisk(*flushed_part_path, metadata_snapshot);
    }

    return src_part->data_part_storage;
}

bool MergeTreeDataPartCloner::doesStoragePolicyAllowSameDisk() const
{
    for (const DiskPtr & disk : merge_tree_data->getStoragePolicy()->getDisks())
    {
        if (disk->getName() == src_part->data_part_storage->getDiskName())
        {
            return true;
        }
    }
    return false;
}

void MergeTreeDataPartCloner::reserveSpaceOnDisk() const
{
    src_part->data_part_storage->reserve(src_part->getBytesOnDisk());
}

std::shared_ptr<IDataPartStorage> MergeTreeDataPartCloner::hardlinkAllFiles(
        const DataPartStoragePtr & storage,
        const String & path
) const
{
    return storage->freeze(
            merge_tree_data->getRelativeDataPath(),
            path,
            false /* make_source_readonly */,
            {},
            false /* copy_instead_of_hardlinks */,
            files_to_copy_instead_of_hardlinks
    );
}

std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> MergeTreeDataPartCloner::cloneSourcePart() const
{
    const String dst_part_name = src_part->getNewName(dst_part_info);

    const String tmp_dst_part_name = tmp_part_prefix + dst_part_name;

    auto temporary_directory_lock = merge_tree_data->getTemporaryPartDirectoryHolder(tmp_dst_part_name);

    reserveSpaceOnDisk();

    auto src_part_storage = flushPartStorageToDiskIfInMemory();

    auto dst_part_storage = hardlinkAllFiles(src_part_storage, tmp_dst_part_name);

    return std::make_pair(merge_tree_data->createPart(dst_part_name, dst_part_info, dst_part_storage), std::move(temporary_directory_lock));
}

void MergeTreeDataPartCloner::handleHardLinkedParameterFiles() const
{
    hardlinked_files->source_part_name = src_part->name;
    hardlinked_files->source_table_shared_id = src_part->storage.getTableSharedID();

    for (auto it = src_part->data_part_storage->iterate(); it->isValid(); it->next())
    {
        if (!files_to_copy_instead_of_hardlinks.contains(it->name())
            && it->name() != IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME
            && it->name() != IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME)
            hardlinked_files->hardlinks_from_source_part.insert(it->name());
    }
}

MergeTreeDataPartCloner::MutableDataPartPtr MergeTreeDataPartCloner::finalizePart(const MutableDataPartPtr & dst_part) const
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
