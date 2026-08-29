#include <Storages/Freeze.h>

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/PartitionCommands.h>
#include <Interpreters/Context.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>

/**
 * When ClickHouse has frozen data on remote storage it required 'smart' data removing during UNFREEZE.
 * For remote storage actually frozen not remote data but local metadata with referrers on remote data.
 * So remote data can be referred from working and frozen data sets (or two frozen) at same time.
 * In this case during UNFREEZE ClickHouse should remove only local metadata and keep remote data.
 * But when data was already removed from working data set ClickHouse should remove remote data too.
 * To detect is current data used or not in some other place ClickHouse uses
 *   - ref_count from metadata to check if data used in some other metadata on the same replica;
 *   - Keeper record to check if data used on other replica.
 * StorageReplicatedMergeTree::removeSharedDetachedPart makes required checks, so here this method
 * called for each frozen part.
 */

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_UUID;
    extern const int SUPPORT_IS_DISABLED;
}

void FreezeMetaData::fill(const StorageReplicatedMergeTree & storage)
{
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
    if (version == 1)
    {
        /// is_replicated and is_remote are not used
        bool is_replicated = true;
        writeBoolText(is_replicated, buffer);
        buffer.write("\n", 1);
        bool is_remote = true;
        writeBoolText(is_remote, buffer);
        buffer.write("\n", 1);
    }
    writeString(escapeForFileName(replica_name), buffer);
    buffer.write("\n", 1);
    writeString(zookeeper_name, buffer);
    buffer.write("\n", 1);
    writeString(table_shared_id, buffer);
    buffer.write("\n", 1);

    tx->writeStringToFile(file_path, buffer.str());
    tx->commit(NoCommitOptions{});
}

bool FreezeMetaData::load(DiskPtr data_disk, const String & path)
{
    auto metadata_storage = data_disk->getMetadataStorage();
    auto file_path = getFileName(path);

    if (!metadata_storage->existsFile(file_path))
        return false;
    auto metadata_str = metadata_storage->readFileToString(file_path);
    ReadBufferFromString buffer(metadata_str);
    readIntText(version, buffer);
    if (version < 1 || version > 2)
    {
        LOG_ERROR(getLogger("FreezeMetaData"), "Unknown frozen metadata version: {}", version);
        return false;
    }
    DB::assertChar('\n', buffer);
    if (version == 1)
    {
        /// is_replicated and is_remote are not used
        bool is_replicated = false;
        readBoolText(is_replicated, buffer);
        DB::assertChar('\n', buffer);
        bool is_remote = false;
        readBoolText(is_remote, buffer);
        DB::assertChar('\n', buffer);
    }
    std::string unescaped_replica_name;
    readString(unescaped_replica_name, buffer);
    replica_name = unescapeForFileName(unescaped_replica_name);
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
    if (metadata_storage->existsFile(fname))
    {
        auto tx = metadata_storage->createTransaction();
        tx->unlinkFile(fname, /*if_exists=*/false, /*should_remove_objects=*/true);
        tx->commit(NoCommitOptions{});
    }
}

String FreezeMetaData::getFileName(const String & path)
{
    return fs::path(path) / "frozen_metadata.txt";
}

Unfreezer::Unfreezer(ContextPtr context) : local_context(context)
{
    if (local_context->hasZooKeeper())
        zookeeper = local_context->getZooKeeper();
}

BlockIO Unfreezer::systemUnfreeze(const String & backup_name)
{
    auto component_guard = Coordination::setCurrentComponent("Unfreezer::systemUnfreeze");
    LOG_DEBUG(log, "Unfreezing backup {}", escapeForFileName(backup_name));

    const auto & config = local_context->getConfigRef();
    static constexpr auto config_key = "enable_system_unfreeze";
    if (!config.getBool(config_key, false))
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                        "Support for SYSTEM UNFREEZE query is disabled. You can enable it via '{}' server setting",
                        config_key);
    }

    auto backup_path = fs::path(backup_directory_prefix) / escapeForFileName(backup_name);
    auto store_paths = {backup_path / "store", backup_path / "data"};

    PartitionCommandsResultInfo result_info;

    /// Table directories of the snapshot, resolved to their storage where possible, collected for
    /// every disk before anything is removed.
    struct SnapshotTableDirectory
    {
        DiskPtr disk;
        fs::path table_directory;
        std::shared_ptr<MergeTreeData> merge_tree;
    };
    std::vector<SnapshotTableDirectory> table_directories;

    auto disks_map = local_context->getDisksMap();

    /// First pass: resolve every table directory and reject the whole command if any of them
    /// belongs to a `leader_election` table that cannot be fenced on this node. The scan covers
    /// all disks before the second pass removes anything, so a rejected `SYSTEM UNFREEZE` leaves
    /// the snapshot exactly as it was.
    for (auto & [_, disk] : disks_map)
    {
        if (disk->isReadOnly() || disk->isWriteOnce())
            continue;

        for (const auto & store_path : store_paths)
        {
            if (!disk->existsDirectory(store_path))
                continue;

            for (auto prefix_it = disk->iterateDirectory(store_path); prefix_it->isValid(); prefix_it->next())
            {
                auto prefix_directory = store_path / prefix_it->name();
                for (auto table_it = disk->iterateDirectory(prefix_directory); table_it->isValid(); table_it->next())
                {
                    auto table_directory = prefix_directory / table_it->name();

                    /// `SYSTEM UNFREEZE` receives only the on-disk path, unlike `ALTER TABLE ...
                    /// UNFREEZE`. Resolve the UUID directory back to its storage before deleting
                    /// anything, so leader-election tables get the same admission-epoch fence.
                    std::shared_ptr<MergeTreeData> merge_tree;
                    try
                    {
                        const UUID table_uuid = parseFromString<UUID>(table_it->name());
                        auto storage = DatabaseCatalog::instance().tryGetByUUID(table_uuid).second;
                        if (storage)
                            merge_tree = std::dynamic_pointer_cast<MergeTreeData>(storage);
                    }
                    catch (const Exception & e)
                    {
                        /// Legacy `data/` paths need not be UUID directories. Preserve their
                        /// existing behavior; UUID paths, which are used by leader election, are
                        /// either resolved above or rejected below, before any deletion.
                        if (e.code() != ErrorCodes::CANNOT_PARSE_UUID)
                            throw;
                    }

                    /// A missing UUID mapping means the table is not loaded on this node: it was
                    /// dropped locally, or never attached here. Removing the snapshot then still
                    /// makes sense for an ordinary table — that is exactly what `SYSTEM UNFREEZE`
                    /// is for — but a `leader_election` snapshot lives on storage shared with
                    /// other nodes and there is no lease left to check here, so the command must
                    /// fail closed instead of deleting data another node's leader owns. The
                    /// snapshot carries the marker written at `FREEZE` time for exactly this.
                    if (!merge_tree
                        && disk->existsFile(table_directory / MergeTreeData::LEADER_ELECTION_SNAPSHOT_MARKER_FILE_NAME))
                    {
                        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                            "Snapshot {} on disk {} belongs to a `leader_election` table that is not attached on this "
                            "node, so its lease cannot be checked here. Removing it could delete shared data owned by "
                            "the current leader. Attach the table on this node and use `ALTER TABLE ... UNFREEZE WITH "
                            "NAME` on the leader instead.",
                            table_directory.generic_string(), disk->getName());
                    }

                    table_directories.push_back({disk, std::move(table_directory), std::move(merge_tree)});
                }
            }
        }
    }

    /// Second pass: the actual removal, fenced per table at the epoch captured at admission.
    std::unordered_map<String, std::vector<std::function<void()>>> assert_writable_at_admission_epoch;
    for (const auto & entry : table_directories)
    {
        const UInt64 admission_epoch = entry.merge_tree ? entry.merge_tree->currentLeadershipEpoch() : 0;
        auto merge_tree = entry.merge_tree;
        auto assert_table_writable = [merge_tree, admission_epoch]
        {
            if (merge_tree)
                merge_tree->assertWritableLeaderAtEpoch(admission_epoch);
        };
        auto current_result_info = unfreezePartitionsFromTableDirectory(
            [](const String &) { return true; }, backup_name, {entry.disk}, entry.table_directory, assert_table_writable);
        assert_writable_at_admission_epoch[entry.disk->getName()].emplace_back(std::move(assert_table_writable));
        for (auto & command_result : current_result_info)
            command_result.command_type = "SYSTEM UNFREEZE";
        result_info.insert(
            result_info.end(),
            std::make_move_iterator(current_result_info.begin()),
            std::make_move_iterator(current_result_info.end()));
    }

    for (auto & [_, disk] : disks_map)
    {
        if (disk->isReadOnly() || disk->isWriteOnce())
            continue;

        if (disk->existsDirectory(backup_path))
        {
            /// After unfreezing we need to clear revision.txt file and empty directories
            for (const auto & assert_writable : assert_writable_at_admission_epoch[disk->getName()])
                assert_writable();
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

bool Unfreezer::removeFrozenPart(DiskPtr disk, const String & path, const String & part_name, ContextPtr local_context, zkutil::ZooKeeperPtr zookeeper)
{
    if (disk->supportZeroCopyReplication())
    {
        FreezeMetaData meta;
        if (meta.load(disk, path))
        {
            FreezeMetaData::clean(disk, path);
            return StorageReplicatedMergeTree::removeSharedDetachedPart(disk, path, part_name, meta.table_shared_id, meta.replica_name, "", local_context, zookeeper);
        }
    }

    disk->removeRecursive(path);

    return false;
}

PartitionCommandsResultInfo Unfreezer::unfreezePartitionsFromTableDirectory(
    MergeTreeData::MatcherFn matcher,
    const String & backup_name,
    const Disks & disks,
    const fs::path & table_directory,
    const std::function<void()> & assert_writable_at_admission_epoch)
{
    PartitionCommandsResultInfo result;

    for (const auto & disk : disks)
    {
        if (!disk->existsDirectory(table_directory))
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

            /// `UNFREEZE` removes from `shadow/` on the table disk. Do not let a leader that
            /// lost, then possibly reacquired, its lease continue deleting another epoch's data.
            assert_writable_at_admission_epoch();
            bool keep_shared = removeFrozenPart(disk, path, partition_directory, local_context, zookeeper);

            result.push_back(PartitionCommandResultInfo{
                .command_type = "UNFREEZE PART",
                .partition_id = partition_id,
                .part_name = partition_directory,
                .backup_path = disk->getPath() + table_directory.generic_string(),
                .part_backup_path = disk->getPath() + path,
                .backup_name = backup_name,
            });

            LOG_DEBUG(log, "Unfrozen part by path {}, keep shared data: {}", disk->getPath() + path, keep_shared);
        }

        /// The leader-election marker is not a part directory, so the loop above never removes it.
        /// Drop it once the snapshot of this table holds no parts any more, so that an emptied
        /// snapshot directory does not keep blocking a later `SYSTEM UNFREEZE`.
        const auto marker_path = table_directory / MergeTreeData::LEADER_ELECTION_SNAPSHOT_MARKER_FILE_NAME;
        if (disk->existsFile(marker_path))
        {
            bool has_parts = false;
            for (auto it = disk->iterateDirectory(table_directory); it->isValid() && !has_parts; it->next())
                has_parts = it->name().find('_') != std::string::npos;

            if (!has_parts)
            {
                assert_writable_at_admission_epoch();
                disk->removeFileIfExists(marker_path);
            }
        }
    }

    LOG_DEBUG(log, "Unfrozen {} parts", result.size());

    return result;
}
}
