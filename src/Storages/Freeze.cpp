#include <Storages/Freeze.h>

#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Storages/PartitionCommands.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>

namespace DB
{
void FreezeMetaData::fill(const StorageReplicatedMergeTree & storage)
{
    is_replicated = storage.supportsReplication();
    is_remote = storage.isRemote();
    replica_name = storage.getReplicaName();
    zookeeper_name = storage.getZooKeeperName();
    table_shared_id = storage.getTableSharedID();
}

void FreezeMetaData::save(DiskPtr data_disk, const String & path) const
{
    auto metadata_storage = data_disk->getMetadataStorage();

    auto file_path = getFileName(path);
    auto tx = metadata_storage->createTransaction();
    WriteBufferFromOwnString buffer;

    writeIntText(version, buffer);
    buffer.write("\n", 1);
    writeBoolText(is_replicated, buffer);
    buffer.write("\n", 1);
    writeBoolText(is_remote, buffer);
    buffer.write("\n", 1);
    writeString(replica_name, buffer);
    buffer.write("\n", 1);
    writeString(zookeeper_name, buffer);
    buffer.write("\n", 1);
    writeString(table_shared_id, buffer);
    buffer.write("\n", 1);

    tx->writeStringToFile(file_path, buffer.str());
    tx->commit();
}

bool FreezeMetaData::load(DiskPtr data_disk, const String & path)
{
    auto metadata_storage = data_disk->getMetadataStorage();
    auto file_path = getFileName(path);

    if (!metadata_storage->exists(file_path))
        return false;
    auto metadata_str = metadata_storage->readFileToString(file_path);
    ReadBufferFromString buffer(metadata_str);
    readIntText(version, buffer);
    if (version != 1)
    {
        LOG_ERROR(&Poco::Logger::get("FreezeMetaData"), "Unknown freezed metadata version: {}", version);
        return false;
    }
    DB::assertChar('\n', buffer);
    readBoolText(is_replicated, buffer);
    DB::assertChar('\n', buffer);
    readBoolText(is_remote, buffer);
    DB::assertChar('\n', buffer);
    readString(replica_name, buffer);
    DB::assertChar('\n', buffer);
    readString(zookeeper_name, buffer);
    DB::assertChar('\n', buffer);
    readString(table_shared_id, buffer);
    DB::assertChar('\n', buffer);
    return true;
}

void FreezeMetaData::clean(DiskPtr data_disk, const String & path)
{
    auto metadata_storage = data_disk->getMetadataStorage();
    auto fname = getFileName(path);
    if (metadata_storage->exists(fname))
    {
        auto tx = metadata_storage->createTransaction();
        tx->unlinkFile(fname);
        tx->commit();
    }
}

String FreezeMetaData::getFileName(const String & path)
{
    return fs::path(path) / "frozen_metadata.txt";
}

BlockIO Unfreezer::unfreeze(const String & backup_name, ContextPtr local_context)
{
    LOG_DEBUG(log, "Unfreezing backup {}", backup_name);
    auto disks_map = local_context->getDisksMap();
    Disks disks;
    for (auto & [name, disk]: disks_map)
    {
        disks.push_back(disk);
    }
    auto backup_path = fs::path(backup_directory_prefix) / escapeForFileName(backup_name);
    auto store_path = backup_path / "store";

    PartitionCommandsResultInfo result_info;

    for (const auto & disk: disks)
    {
        if (!disk->exists(store_path))
            continue;
        for (auto prefix_it = disk->iterateDirectory(store_path); prefix_it->isValid(); prefix_it->next())
        {
            auto prefix_directory = store_path / prefix_it->name();
            for (auto table_it = disk->iterateDirectory(prefix_directory); table_it->isValid(); table_it->next())
            {
                auto table_directory = prefix_directory / table_it->name();
                auto current_result_info = unfreezePartitionsFromTableDirectory([] (const String &) { return true; }, backup_name, {disk}, table_directory, local_context);
                for (auto & command_result : current_result_info)
                {
                    command_result.command_type = "SYSTEM UNFREEZE";
                }
                result_info.insert(
                                result_info.end(),
                                std::make_move_iterator(current_result_info.begin()),
                                std::make_move_iterator(current_result_info.end()));
            }
        }
        if (disk->exists(backup_path))
        {
            disk->removeRecursive(backup_path);
        }
    }

    BlockIO result;
    if (!result_info.empty())
    {
        result.pipeline = QueryPipeline(convertCommandsResultToSource(result_info));
    }
    return result;
}

bool Unfreezer::removeFreezedPart(DiskPtr disk, const String & path, const String & part_name, ContextPtr local_context)
{
    if (disk->supportZeroCopyReplication())
    {
        FreezeMetaData meta;
        if (meta.load(disk, path))
        {
            if (meta.is_replicated)
            {
                FreezeMetaData::clean(disk, path);
                return StorageReplicatedMergeTree::removeSharedDetachedPart(disk, path, part_name, meta.table_shared_id, meta.zookeeper_name, meta.replica_name, "", local_context);
            }
        }
    }

    disk->removeRecursive(path);

    return false;
}

PartitionCommandsResultInfo Unfreezer::unfreezePartitionsFromTableDirectory(MergeTreeData::MatcherFn matcher, const String & backup_name, const Disks & disks, const fs::path & table_directory, ContextPtr local_context)
{
    PartitionCommandsResultInfo result;

    for (const auto & disk : disks)
    {
        if (!disk->exists(table_directory))
            continue;

        for (auto it = disk->iterateDirectory(table_directory); it->isValid(); it->next())
        {
            const auto & partition_directory = it->name();

            /// Partition ID is prefix of part directory name: <partition id>_<rest of part directory name>
            auto found = partition_directory.find('_');
            if (found == std::string::npos)
                continue;
            auto partition_id = partition_directory.substr(0, found);

            if (!matcher(partition_id))
                continue;

            const auto & path = it->path();

            bool keep_shared = removeFreezedPart(disk, path, partition_directory, local_context);

            result.push_back(PartitionCommandResultInfo{
                .partition_id = partition_id,
                .part_name = partition_directory,
                .backup_path = disk->getPath() + table_directory.generic_string(),
                .part_backup_path = disk->getPath() + path,
                .backup_name = backup_name,
            });

            LOG_DEBUG(log, "Unfreezed part by path {}, keep shared data: {}", disk->getPath() + path, keep_shared);
        }
    }

    LOG_DEBUG(log, "Unfreezed {} parts", result.size());

    return result;
}
}
