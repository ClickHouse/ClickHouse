#include <Interpreters/MergeTreeTransaction.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeDataPartCloner.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DistinctPartitionExpression
{
std::unique_ptr<WriteBufferFromFileBase> updatePartitionFile(
    const MergeTreeData & merge_tree_data,
    const MergeTreePartition & partition,
    const MergeTreeData::MutableDataPartPtr & dst_part,
    IDataPartStorage & storage)
{
    storage.removeFile("partition.dat");
    // Leverage already implemented MergeTreePartition::store to create & store partition.dat.
    // Checksum is re-calculated later.
    return partition.store(merge_tree_data, storage, dst_part->checksums);
}

IMergeTreeDataPart::MinMaxIndex::WrittenFiles updateMinMaxFiles(
    const MergeTreeData & merge_tree_data,
    const MergeTreeData::MutableDataPartPtr & dst_part,
    IDataPartStorage & storage,
    const StorageMetadataPtr & metadata_snapshot)
{
    for (const auto & column_name : MergeTreeData::getMinMaxColumnsNames(metadata_snapshot->partition_key))
    {
        auto file = "minmax_" + escapeForFileName(column_name) + ".idx";
        storage.removeFile(file);
    }

    return dst_part->minmax_idx->store(merge_tree_data, storage, dst_part->checksums);
}

void finalizeNewFiles(const std::vector<std::unique_ptr<WriteBufferFromFileBase>> & files, bool sync_new_files)
{
    for (const auto & file : files)
    {
        file->finalize();
        if (sync_new_files)
            file->sync();
    }
}

void updateNewPartFiles(
    const MergeTreeData & merge_tree_data,
    const MergeTreeData::MutableDataPartPtr & dst_part,
    const MergeTreePartition & new_partition,
    const IMergeTreeDataPart::MinMaxIndex & new_min_max_index,
    const StorageMetadataPtr & src_metadata_snapshot,
    bool sync_new_files)
{
    auto & storage = dst_part->getDataPartStorage();

    *dst_part->minmax_idx = new_min_max_index;

    auto partition_file = updatePartitionFile(merge_tree_data, new_partition, dst_part, storage);

    auto min_max_files = updateMinMaxFiles(merge_tree_data, dst_part, storage, src_metadata_snapshot);

    IMergeTreeDataPart::MinMaxIndex::WrittenFiles written_files;

    if (partition_file)
        written_files.emplace_back(std::move(partition_file));

    written_files.insert(written_files.end(), std::make_move_iterator(min_max_files.begin()), std::make_move_iterator(min_max_files.end()));

    finalizeNewFiles(written_files, sync_new_files);

    // MergeTreeDataPartCloner::finalize_part calls IMergeTreeDataPart::loadColumnsChecksumsIndexes, which will re-create
    // the checksum file if it doesn't exist. Relying on that is cumbersome, but this refactoring is simply a code extraction
    // with small improvements. It can be further improved in the future.
    storage.removeFile("checksums.txt");
}
}

namespace
{
bool doesStoragePolicyAllowSameDisk(MergeTreeData * merge_tree_data, const MergeTreeData::DataPartPtr & src_part)
{
    for (const DiskPtr & disk : merge_tree_data->getStoragePolicy()->getDisks())
        if (disk->getName() == src_part->getDataPartStorage().getDiskName())
            return true;
    return false;
}

DataPartStoragePtr flushPartStorageToDiskIfInMemory(
    MergeTreeData * merge_tree_data,
    const MergeTreeData::DataPartPtr & src_part,
    const StorageMetadataPtr & metadata_snapshot,
    const String & tmp_part_prefix,
    const String & tmp_dst_part_name,
    scope_guard & src_flushed_tmp_dir_lock,
    MergeTreeData::MutableDataPartPtr src_flushed_tmp_part)
{
    if (auto src_part_in_memory = asInMemoryPart(src_part))
    {
        auto flushed_part_path = src_part_in_memory->getRelativePathForPrefix(tmp_part_prefix);
        auto tmp_src_part_file_name = fs::path(tmp_dst_part_name).filename();

        src_flushed_tmp_dir_lock = src_part->storage.getTemporaryPartDirectoryHolder(tmp_src_part_file_name);

        auto flushed_part_storage = src_part_in_memory->flushToDisk(*flushed_part_path, metadata_snapshot);

        src_flushed_tmp_part = MergeTreeDataPartBuilder(*merge_tree_data, src_part->name, flushed_part_storage)
                                   .withPartInfo(src_part->info)
                                   .withPartFormatFromDisk()
                                   .build();

        src_flushed_tmp_part->is_temp = true;

        return flushed_part_storage;
    }

    return src_part->getDataPartStoragePtr();
}

std::shared_ptr<IDataPartStorage> hardlinkAllFiles(
    MergeTreeData * merge_tree_data,
    const DB::ReadSettings & read_settings,
    const DB::WriteSettings & write_settings,
    const DataPartStoragePtr & storage,
    const String & path,
    const DB::IDataPartStorage::ClonePartParams & params)
{
    return storage->freeze(
        merge_tree_data->getRelativeDataPath(),
        path,
        read_settings,
        write_settings,
        /*save_metadata_callback=*/{},
        params);
}

std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> cloneSourcePart(
    MergeTreeData * merge_tree_data,
    const MergeTreeData::DataPartPtr & src_part,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreePartInfo & dst_part_info,
    const String & tmp_part_prefix,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const DB::IDataPartStorage::ClonePartParams & params)
{
    const auto dst_part_name = src_part->getNewName(dst_part_info);

    const auto tmp_dst_part_name = tmp_part_prefix + dst_part_name;

    auto temporary_directory_lock = merge_tree_data->getTemporaryPartDirectoryHolder(tmp_dst_part_name);

    src_part->getDataPartStorage().reserve(src_part->getBytesOnDisk());

    scope_guard src_flushed_tmp_dir_lock;
    MergeTreeData::MutableDataPartPtr src_flushed_tmp_part;

    auto src_part_storage = flushPartStorageToDiskIfInMemory(
        merge_tree_data, src_part, metadata_snapshot, tmp_part_prefix, tmp_dst_part_name, src_flushed_tmp_dir_lock, src_flushed_tmp_part);

    auto dst_part_storage = hardlinkAllFiles(merge_tree_data, read_settings, write_settings, src_part_storage, tmp_dst_part_name, params);

    if (params.metadata_version_to_write.has_value())
    {
        chassert(!params.keep_metadata_version);
        auto out_metadata = dst_part_storage->writeFile(
            IMergeTreeDataPart::METADATA_VERSION_FILE_NAME, 4096, merge_tree_data->getContext()->getWriteSettings());
        writeText(metadata_snapshot->getMetadataVersion(), *out_metadata);
        out_metadata->finalize();
        if (merge_tree_data->getSettings()->fsync_after_insert)
            out_metadata->sync();
    }

    LOG_DEBUG(
        &Poco::Logger::get("MergeTreeDataPartCloner"),
        "Clone {} part {} to {}{}",
        src_flushed_tmp_part ? "flushed" : "",
        src_part_storage->getFullPath(),
        std::string(fs::path(dst_part_storage->getFullRootPath()) / tmp_dst_part_name),
        false);


    auto part = MergeTreeDataPartBuilder(*merge_tree_data, dst_part_name, dst_part_storage).withPartFormatFromDisk().build();

    return std::make_pair(part, std::move(temporary_directory_lock));
}

void handleHardLinkedParameterFiles(const MergeTreeData::DataPartPtr & src_part, const DB::IDataPartStorage::ClonePartParams & params)
{
    const auto & hardlinked_files = params.hardlinked_files;

    hardlinked_files->source_part_name = src_part->name;
    hardlinked_files->source_table_shared_id = src_part->storage.getTableSharedID();

    for (auto it = src_part->getDataPartStorage().iterate(); it->isValid(); it->next())
    {
        if (!params.files_to_copy_instead_of_hardlinks.contains(it->name())
            && it->name() != IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME_DEPRECATED
            && it->name() != IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME)
        {
            hardlinked_files->hardlinks_from_source_part.insert(it->name());
        }
    }
}

void handleProjections(const MergeTreeData::DataPartPtr & src_part, const DB::IDataPartStorage::ClonePartParams & params)
{
    auto projections = src_part->getProjectionParts();
    for (const auto & [name, projection_part] : projections)
    {
        const auto & projection_storage = projection_part->getDataPartStorage();
        for (auto it = projection_storage.iterate(); it->isValid(); it->next())
        {
            auto file_name_with_projection_prefix = fs::path(projection_storage.getPartDirectory()) / it->name();
            if (!params.files_to_copy_instead_of_hardlinks.contains(file_name_with_projection_prefix)
                && it->name() != IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME_DEPRECATED
                && it->name() != IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME)
            {
                params.hardlinked_files->hardlinks_from_source_part.insert(file_name_with_projection_prefix);
            }
        }
    }
}

MergeTreeData::MutableDataPartPtr finalizePart(
    const MergeTreeData::MutableDataPartPtr & dst_part, const DB::IDataPartStorage::ClonePartParams & params, bool require_part_metadata)
{
    /// We should write version metadata on part creation to distinguish it from parts that were created without transaction.
    TransactionID tid = params.txn ? params.txn->tid : Tx::PrehistoricTID;
    dst_part->version.setCreationTID(tid, nullptr);
    dst_part->storeVersionMetadata();

    dst_part->is_temp = true;

    dst_part->loadColumnsChecksumsIndexes(require_part_metadata, true);

    dst_part->modification_time = dst_part->getDataPartStorage().getLastModified().epochTime();

    return dst_part;
}

std::pair<MergeTreeDataPartCloner::MutableDataPartPtr, scope_guard> cloneAndHandleHardlinksAndProjections(
    MergeTreeData * merge_tree_data,
    const DataPartPtr & src_part,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreePartInfo & dst_part_info,
    const String & tmp_part_prefix,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const IDataPartStorage::ClonePartParams & params)
{
    chassert(!merge_tree_data->isStaticStorage());
    if (!doesStoragePolicyAllowSameDisk(merge_tree_data, src_part))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Could not clone and load part {} because disk does not belong to storage policy",
            quoteString(src_part->getDataPartStorage().getFullPath()));

    auto [destination_part, temporary_directory_lock] = cloneSourcePart(
        merge_tree_data, src_part, metadata_snapshot, dst_part_info, tmp_part_prefix, read_settings, write_settings, params);

    if (!params.copy_instead_of_hardlink && params.hardlinked_files)
    {
        handleHardLinkedParameterFiles(src_part, params);
        handleProjections(src_part, params);
    }

    return std::make_pair(destination_part, std::move(temporary_directory_lock));
}
}

std::pair<MergeTreeDataPartCloner::MutableDataPartPtr, scope_guard> MergeTreeDataPartCloner::clone(
    MergeTreeData * merge_tree_data,
    const DataPartPtr & src_part,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreePartInfo & dst_part_info,
    const String & tmp_part_prefix,
    bool require_part_metadata,
    const IDataPartStorage::ClonePartParams & params,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings)
{
    auto [destination_part, temporary_directory_lock] = cloneAndHandleHardlinksAndProjections(
        merge_tree_data, src_part, metadata_snapshot, dst_part_info, tmp_part_prefix, read_settings, write_settings, params);

    return std::make_pair(finalizePart(destination_part, params, require_part_metadata), std::move(temporary_directory_lock));
}

std::pair<MergeTreeDataPartCloner::MutableDataPartPtr, scope_guard> MergeTreeDataPartCloner::cloneWithDistinctPartitionExpression(
    MergeTreeData * merge_tree_data,
    const DataPartPtr & src_part,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreePartInfo & dst_part_info,
    const String & tmp_part_prefix,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const MergeTreePartition & new_partition,
    const IMergeTreeDataPart::MinMaxIndex & new_min_max_index,
    bool sync_new_files,
    const IDataPartStorage::ClonePartParams & params)
{
    auto [destination_part, temporary_directory_lock] = cloneAndHandleHardlinksAndProjections(
        merge_tree_data, src_part, metadata_snapshot, dst_part_info, tmp_part_prefix, read_settings, write_settings, params);

    DistinctPartitionExpression::updateNewPartFiles(
        *merge_tree_data, destination_part, new_partition, new_min_max_index, src_part->storage.getInMemoryMetadataPtr(), sync_new_files);

    return std::make_pair(finalizePart(destination_part, params, false), std::move(temporary_directory_lock));
}

}
