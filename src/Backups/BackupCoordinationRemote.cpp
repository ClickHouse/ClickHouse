#include <Backups/BackupCoordinationRemote.h>
#include <Access/Common/AccessEntityType.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <base/hex.h>
#include <Backups/BackupCoordinationStage.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_NODE_IN_ZOOKEEPER;
    extern const int LOGICAL_ERROR;
    extern const int BACKUP_ENTRY_ALREADY_EXISTS;
}

namespace Stage = BackupCoordinationStage;

namespace
{
    using FileInfo = IBackupCoordination::FileInfo;
    using SizeAndChecksum = FileInfo::SizeAndChecksum;
    using PartNameAndChecksum = IBackupCoordination::PartNameAndChecksum;
    using MutationInfo = IBackupCoordination::MutationInfo;

    struct ReplicatedPartNames
    {
        std::vector<PartNameAndChecksum> part_names_and_checksums;
        String table_name_for_logs;

        static String serialize(const std::vector<PartNameAndChecksum> & part_names_and_checksums_, const String & table_name_for_logs_)
        {
            WriteBufferFromOwnString out;
            writeBinary(part_names_and_checksums_.size(), out);
            for (const auto & part_name_and_checksum : part_names_and_checksums_)
            {
                writeBinary(part_name_and_checksum.part_name, out);
                writeBinary(part_name_and_checksum.checksum, out);
            }
            writeBinary(table_name_for_logs_, out);
            return out.str();
        }

        static ReplicatedPartNames deserialize(const String & str)
        {
            ReadBufferFromString in{str};
            ReplicatedPartNames res;
            size_t num;
            readBinary(num, in);
            res.part_names_and_checksums.resize(num);
            for (size_t i = 0; i != num; ++i)
            {
                readBinary(res.part_names_and_checksums[i].part_name, in);
                readBinary(res.part_names_and_checksums[i].checksum, in);
            }
            readBinary(res.table_name_for_logs, in);
            return res;
        }
    };

    struct ReplicatedMutations
    {
        std::vector<MutationInfo> mutations;
        String table_name_for_logs;

        static String serialize(const std::vector<MutationInfo> & mutations_, const String & table_name_for_logs_)
        {
            WriteBufferFromOwnString out;
            writeBinary(mutations_.size(), out);
            for (const auto & mutation : mutations_)
            {
                writeBinary(mutation.id, out);
                writeBinary(mutation.entry, out);
            }
            writeBinary(table_name_for_logs_, out);
            return out.str();
        }

        static ReplicatedMutations deserialize(const String & str)
        {
            ReadBufferFromString in{str};
            ReplicatedMutations res;
            size_t num;
            readBinary(num, in);
            res.mutations.resize(num);
            for (size_t i = 0; i != num; ++i)
            {
                readBinary(res.mutations[i].id, in);
                readBinary(res.mutations[i].entry, in);
            }
            readBinary(res.table_name_for_logs, in);
            return res;
        }
    };

    /// Serialize only `file_name`, `base_size`, `base_checksum` of a specified FileInfo.
    String serializeDataFileNameWithBaseInfo(const FileInfo & info)
    {
        WriteBufferFromOwnString out;
        writeBinary(info.data_file_name, out);
        writeBinary(info.base_size, out);
        writeBinary(info.base_checksum, out);
        return out.str();
    }

    /// Deserialize only `file_name`, `base_size`, `base_checksum` of a specified FileInfo.
    void deserializeDataFileNameWithBaseInfo(const String & str, FileInfo & res)
    {
        ReadBufferFromString in{str};
        readBinary(res.data_file_name, in);
        readBinary(res.base_size, in);
        readBinary(res.base_checksum, in);
    }

    /// Serialize a size and a checksum as "<size>_<checksum".
    String serializeSizeAndChecksum(const SizeAndChecksum & size_and_checksum)
    {
        return std::to_string(size_and_checksum.first) + "_" + getHexUIntLowercase(size_and_checksum.second);
    }

    /// Deserialize a size and a checksum from a string formatted as "<size>_<checksum".
    SizeAndChecksum deserializeSizeAndChecksum(const std::string_view & str)
    {
        size_t underscore = str.find('_');
        constexpr size_t checksum_length = sizeof(UInt128) * 2;
        if (underscore == String::npos)
            throw Exception(ErrorCodes::UNEXPECTED_NODE_IN_ZOOKEEPER, "Unexpected underscore in {}", str);
        if (underscore + 1 + checksum_length != str.length())
            throw Exception(
                ErrorCodes::UNEXPECTED_NODE_IN_ZOOKEEPER, "Unexpected size of checksum in {}, must be {}", str, checksum_length);
        UInt64 size = parseFromString<UInt64>(str.substr(0, underscore));
        UInt128 checksum = unhexUInt<UInt128>(&str[underscore + 1]);
        return std::pair{size, checksum};
    }
}

size_t BackupCoordinationRemote::findCurrentHostIndex(const Strings & all_hosts, const String & current_host)
{
    auto it = std::find(all_hosts.begin(), all_hosts.end(), current_host);
    if (it == all_hosts.end())
        return 0;
    return it - all_hosts.begin();
}

BackupCoordinationRemote::BackupCoordinationRemote(
    zkutil::GetZooKeeper get_zookeeper_,
    const String & root_zookeeper_path_,
    const BackupKeeperSettings & keeper_settings_,
    const String & backup_uuid_,
    const Strings & all_hosts_,
    const String & current_host_,
    bool plain_backup_,
    bool is_internal_)
    : get_zookeeper(get_zookeeper_)
    , root_zookeeper_path(root_zookeeper_path_)
    , zookeeper_path(root_zookeeper_path_ + "/backup-" + backup_uuid_)
    , keeper_settings(keeper_settings_)
    , backup_uuid(backup_uuid_)
    , all_hosts(all_hosts_)
    , current_host(current_host_)
    , current_host_index(findCurrentHostIndex(all_hosts, current_host))
    , plain_backup(plain_backup_)
    , is_internal(is_internal_)
{
    zookeeper_retries_info = ZooKeeperRetriesInfo(
        "BackupCoordinationRemote",
        &Poco::Logger::get("BackupCoordinationRemote"),
        keeper_settings.keeper_max_retries,
        keeper_settings.keeper_retry_initial_backoff_ms,
        keeper_settings.keeper_retry_max_backoff_ms);

    createRootNodes();
    stage_sync.emplace(
        zookeeper_path + "/stage", [this] { return getZooKeeper(); }, &Poco::Logger::get("BackupCoordination"));
}

BackupCoordinationRemote::~BackupCoordinationRemote()
{
    try
    {
        if (!is_internal)
            removeAllNodes();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

zkutil::ZooKeeperPtr BackupCoordinationRemote::getZooKeeper() const
{
    std::lock_guard lock{mutex};
    return getZooKeeperNoLock();
}

zkutil::ZooKeeperPtr BackupCoordinationRemote::getZooKeeperNoLock() const
{
    if (!zookeeper || zookeeper->expired())
    {
        zookeeper = get_zookeeper();

        /// It's possible that we connected to different [Zoo]Keeper instance
        /// so we may read a bit stale state.
        zookeeper->sync(zookeeper_path);
    }
    return zookeeper;
}

void BackupCoordinationRemote::createRootNodes()
{
    auto zk = getZooKeeper();
    zk->createAncestors(zookeeper_path);
    zk->createIfNotExists(zookeeper_path, "");
    zk->createIfNotExists(zookeeper_path + "/repl_part_names", "");
    zk->createIfNotExists(zookeeper_path + "/repl_mutations", "");
    zk->createIfNotExists(zookeeper_path + "/repl_data_paths", "");
    zk->createIfNotExists(zookeeper_path + "/repl_access", "");
    zk->createIfNotExists(zookeeper_path + "/repl_sql_objects", "");
    zk->createIfNotExists(zookeeper_path + "/file_names", "");
    zk->createIfNotExists(zookeeper_path + "/file_infos", "");
}

void BackupCoordinationRemote::removeAllNodes()
{
    /// Usually this function is called by the initiator when a backup is complete so we don't need the coordination anymore.
    ///
    /// However there can be a rare situation when this function is called after an error occurs on the initiator of a query
    /// while some hosts are still making the backup. Removing all the nodes will remove the parent node of the backup coordination
    /// at `zookeeper_path` which might cause such hosts to stop with exception "ZNONODE". Or such hosts might still do some useless part
    /// of their backup work before that. Anyway in this case backup won't be finalized (because only an initiator can do that).
    auto zk = getZooKeeper();
    zk->removeRecursive(zookeeper_path);
}


void BackupCoordinationRemote::setStage(const String & new_stage, const String & message)
{
    stage_sync->set(current_host, new_stage, message);
}

void BackupCoordinationRemote::setError(const Exception & exception)
{
    stage_sync->setError(current_host, exception);
}

Strings BackupCoordinationRemote::waitForStage(const String & stage_to_wait)
{
    return stage_sync->wait(all_hosts, stage_to_wait);
}

Strings BackupCoordinationRemote::waitForStage(const String & stage_to_wait, std::chrono::milliseconds timeout)
{
    return stage_sync->waitFor(all_hosts, stage_to_wait, timeout);
}


void BackupCoordinationRemote::addReplicatedPartNames(
    const String & table_shared_id,
    const String & table_name_for_logs,
    const String & replica_name,
    const std::vector<PartNameAndChecksum> & part_names_and_checksums)
{
    {
        std::lock_guard lock{mutex};
        if (replicated_tables)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedPartNames() must not be called after preparing");
    }

    auto zk = getZooKeeper();
    String path = zookeeper_path + "/repl_part_names/" + escapeForFileName(table_shared_id);
    zk->createIfNotExists(path, "");
    path += "/" + escapeForFileName(replica_name);
    zk->create(path, ReplicatedPartNames::serialize(part_names_and_checksums, table_name_for_logs), zkutil::CreateMode::Persistent);
}

Strings BackupCoordinationRemote::getReplicatedPartNames(const String & table_shared_id, const String & replica_name) const
{
    std::lock_guard lock{mutex};
    prepareReplicatedTables();
    return replicated_tables->getPartNames(table_shared_id, replica_name);
}

void BackupCoordinationRemote::addReplicatedMutations(
    const String & table_shared_id,
    const String & table_name_for_logs,
    const String & replica_name,
    const std::vector<MutationInfo> & mutations)
{
    {
        std::lock_guard lock{mutex};
        if (replicated_tables)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedMutations() must not be called after preparing");
    }

    auto zk = getZooKeeper();
    String path = zookeeper_path + "/repl_mutations/" + escapeForFileName(table_shared_id);
    zk->createIfNotExists(path, "");
    path += "/" + escapeForFileName(replica_name);
    zk->create(path, ReplicatedMutations::serialize(mutations, table_name_for_logs), zkutil::CreateMode::Persistent);
}

std::vector<IBackupCoordination::MutationInfo> BackupCoordinationRemote::getReplicatedMutations(const String & table_shared_id, const String & replica_name) const
{
    std::lock_guard lock{mutex};
    prepareReplicatedTables();
    return replicated_tables->getMutations(table_shared_id, replica_name);
}


void BackupCoordinationRemote::addReplicatedDataPath(
    const String & table_shared_id, const String & data_path)
{
    {
        std::lock_guard lock{mutex};
        if (replicated_tables)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedDataPath() must not be called after preparing");
    }

    auto zk = getZooKeeper();
    String path = zookeeper_path + "/repl_data_paths/" + escapeForFileName(table_shared_id);
    zk->createIfNotExists(path, "");
    path += "/" + escapeForFileName(data_path);
    zk->createIfNotExists(path, "");
}

Strings BackupCoordinationRemote::getReplicatedDataPaths(const String & table_shared_id) const
{
    std::lock_guard lock{mutex};
    prepareReplicatedTables();
    return replicated_tables->getDataPaths(table_shared_id);
}


void BackupCoordinationRemote::prepareReplicatedTables() const
{
    if (replicated_tables)
        return;

    replicated_tables.emplace();
    auto zk = getZooKeeperNoLock();

    {
        String path = zookeeper_path + "/repl_part_names";
        for (const String & escaped_table_shared_id : zk->getChildren(path))
        {
            String table_shared_id = unescapeForFileName(escaped_table_shared_id);
            String path2 = path + "/" + escaped_table_shared_id;
            for (const String & escaped_replica_name : zk->getChildren(path2))
            {
                String replica_name = unescapeForFileName(escaped_replica_name);
                auto part_names = ReplicatedPartNames::deserialize(zk->get(path2 + "/" + escaped_replica_name));
                replicated_tables->addPartNames(table_shared_id, part_names.table_name_for_logs, replica_name, part_names.part_names_and_checksums);
            }
        }
    }

    {
        String path = zookeeper_path + "/repl_mutations";
        for (const String & escaped_table_shared_id : zk->getChildren(path))
        {
            String table_shared_id = unescapeForFileName(escaped_table_shared_id);
            String path2 = path + "/" + escaped_table_shared_id;
            for (const String & escaped_replica_name : zk->getChildren(path2))
            {
                String replica_name = unescapeForFileName(escaped_replica_name);
                auto mutations = ReplicatedMutations::deserialize(zk->get(path2 + "/" + escaped_replica_name));
                replicated_tables->addMutations(table_shared_id, mutations.table_name_for_logs, replica_name, mutations.mutations);
            }
        }
    }

    {
        String path = zookeeper_path + "/repl_data_paths";
        for (const String & escaped_table_shared_id : zk->getChildren(path))
        {
            String table_shared_id = unescapeForFileName(escaped_table_shared_id);
            String path2 = path + "/" + escaped_table_shared_id;
            for (const String & escaped_data_path : zk->getChildren(path2))
            {
                String data_path = unescapeForFileName(escaped_data_path);
                replicated_tables->addDataPath(table_shared_id, data_path);
            }
        }
    }
}


void BackupCoordinationRemote::addReplicatedAccessFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & file_path)
{
    {
        std::lock_guard lock{mutex};
        if (replicated_access)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedAccessFilePath() must not be called after preparing");
    }

    auto zk = getZooKeeper();
    String path = zookeeper_path + "/repl_access/" + escapeForFileName(access_zk_path);
    zk->createIfNotExists(path, "");
    path += "/" + AccessEntityTypeInfo::get(access_entity_type).name;
    zk->createIfNotExists(path, "");
    path += "/" + current_host;
    zk->createIfNotExists(path, file_path);
}

Strings BackupCoordinationRemote::getReplicatedAccessFilePaths(const String & access_zk_path, AccessEntityType access_entity_type) const
{
    std::lock_guard lock{mutex};
    prepareReplicatedAccess();
    return replicated_access->getFilePaths(access_zk_path, access_entity_type, current_host);
}

void BackupCoordinationRemote::prepareReplicatedAccess() const
{
    if (replicated_access)
        return;

    replicated_access.emplace();
    auto zk = getZooKeeperNoLock();

    String path = zookeeper_path + "/repl_access";
    for (const String & escaped_access_zk_path : zk->getChildren(path))
    {
        String access_zk_path = unescapeForFileName(escaped_access_zk_path);
        String path2 = path + "/" + escaped_access_zk_path;
        for (const String & type_str : zk->getChildren(path2))
        {
            AccessEntityType type = AccessEntityTypeInfo::parseType(type_str);
            String path3 = path2 + "/" + type_str;
            for (const String & host_id : zk->getChildren(path3))
            {
                String file_path = zk->get(path3 + "/" + host_id);
                replicated_access->addFilePath(access_zk_path, type, host_id, file_path);
            }
        }
    }
}

void BackupCoordinationRemote::addReplicatedSQLObjectsDir(const String & loader_zk_path, UserDefinedSQLObjectType object_type, const String & dir_path)
{
    {
        std::lock_guard lock{mutex};
        if (replicated_sql_objects)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedSQLObjectsDir() must not be called after preparing");
    }

    auto zk = getZooKeeper();
    String path = zookeeper_path + "/repl_sql_objects/" + escapeForFileName(loader_zk_path);
    zk->createIfNotExists(path, "");

    path += "/";
    switch (object_type)
    {
        case UserDefinedSQLObjectType::Function:
            path += "functions";
            break;
    }

    zk->createIfNotExists(path, "");
    path += "/" + current_host;
    zk->createIfNotExists(path, dir_path);
}

Strings BackupCoordinationRemote::getReplicatedSQLObjectsDirs(const String & loader_zk_path, UserDefinedSQLObjectType object_type) const
{
    std::lock_guard lock{mutex};
    prepareReplicatedSQLObjects();
    return replicated_sql_objects->getDirectories(loader_zk_path, object_type, current_host);
}

void BackupCoordinationRemote::prepareReplicatedSQLObjects() const
{
    if (replicated_sql_objects)
        return;

    replicated_sql_objects.emplace();
    auto zk = getZooKeeperNoLock();

    String path = zookeeper_path + "/repl_sql_objects";
    for (const String & escaped_loader_zk_path : zk->getChildren(path))
    {
        String loader_zk_path = unescapeForFileName(escaped_loader_zk_path);
        String objects_path = path + "/" + escaped_loader_zk_path;

        if (String functions_path = objects_path + "/functions"; zk->exists(functions_path))
        {
            UserDefinedSQLObjectType object_type = UserDefinedSQLObjectType::Function;
            for (const String & host_id : zk->getChildren(functions_path))
            {
                String dir = zk->get(functions_path + "/" + host_id);
                replicated_sql_objects->addDirectory(loader_zk_path, object_type, host_id, dir);
            }
        }
    }
}


void BackupCoordinationRemote::addFileInfo(const FileInfo & file_info, bool & is_data_file_required)
{
    /// file_names/
    ///     escaped_file_name -> size + checksum
    /// file_infos/
    ///     size + checksum -> data_file_name + base_size + base_checksum

    String size_and_checksum = serializeSizeAndChecksum(std::pair{file_info.size, file_info.checksum});

    /// Add a znode to file_names/
    /// with the name "<escaped_file_name>" and the value containing `size` and `checksum`.
    {
        String full_path = zookeeper_path + "/file_names/" + escapeForFileName(file_info.file_name);
        ZooKeeperRetriesControl retries_ctl("addFileInfo::addFileName", zookeeper_retries_info);
        retries_ctl.retryLoop([&]
        {
            auto zk = getZooKeeper();
            zk->createIfNotExists(full_path, size_and_checksum);
        });
    }

    if (plain_backup)
    {
        /// Plain backups don't use base backups and store each file as is.
        is_data_file_required = true;
        return;
    }

    if (file_info.size == 0)
    {
        /// We don't store data files for empty files.
        is_data_file_required = false;
        return;
    }

    /// If a group files in the backup have the same size and checksum then we store only a single data file for that group. 
    {
        /// Add a znode to file_infos/ with the name "<size>_<checksum>" and the value containing `data_file_name`, `base_size`, `base_checksum`.
        String full_path = zookeeper_path + "/file_infos/" + size_and_checksum;
        String data_file_and_base_info = serializeDataFileNameWithBaseInfo(file_info);

        ZooKeeperRetriesControl retries_ctl("addFileInfo::addSizeAndChecksum", zookeeper_retries_info);
        retries_ctl.retryLoop([&]
        {
            auto zk = getZooKeeper();

            auto code = zk->tryCreate(full_path, data_file_and_base_info, zkutil::CreateMode::Persistent);

            if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
                throw zkutil::KeeperException(code, full_path);
            
            bool added_znode = true;
            if (code == Coordination::Error::ZNODEEXISTS)
                added_znode = (zk->get(full_path) == data_file_and_base_info);

            /// We store a data file only if it's not empty and only when it's added for the first time.
            is_data_file_required = added_znode && (file_info.size > file_info.base_size);
        });
    }
}

std::vector<FileInfo> BackupCoordinationRemote::getAllFileInfos() const
{
    /// There could be tons of files inside /file_infos or /file_names
    /// Thus we use MultiRead requests for processing them
    /// We also use [Zoo]Keeper retries and it should be safe, because
    /// this function is called at the end after the actual copying is finished.

    auto split_vector = [](Strings && vec, size_t max_batch_size) -> std::vector<Strings>
    {
        std::vector<Strings> result;
        size_t left_border = 0;

        auto move_to_result = [&](auto && begin, auto && end)
        {
            auto batch = Strings();
            batch.reserve(max_batch_size);
            std::move(begin, end, std::back_inserter(batch));
            result.push_back(std::move(batch));
        };

        if (max_batch_size == 0)
        {
            move_to_result(vec.begin(), vec.end());
            return result;
        }

        for (size_t pos = 0; pos < vec.size(); ++pos)
        {
            if (pos >= left_border + max_batch_size)
            {
                move_to_result(vec.begin() + left_border, vec.begin() + pos);
                left_border = pos;
            }
        }

        if (vec.begin() + left_border != vec.end())
            move_to_result(vec.begin() + left_border, vec.end());

        return result;
    };

    /// Gets all file names from /file_names
    Strings escaped_file_names;
    {
        ZooKeeperRetriesControl retries_ctl("getAllFileInfos::getAllFileNames", zookeeper_retries_info);
        retries_ctl.retryLoop([&]()
        {
            auto zk = getZooKeeper();
            escaped_file_names = zk->getChildren(zookeeper_path + "/file_names");
        });
    }

    std::vector<FileInfo> file_infos;
    file_infos.reserve(escaped_file_names.size());

    /// For each file name we read the size and the checksum.
    auto batches = split_vector(std::move(escaped_file_names), keeper_settings.batch_size_for_keeper_multiread);
    for (auto & batch : batches)
    {
        zkutil::ZooKeeper::MultiGetResponse multi_response;
        {
            Strings paths;
            paths.reserve(batch.size());
            for (const String & escaped_file_name : batch)
                paths.emplace_back(zookeeper_path + "/file_names/" + escaped_file_name);

            ZooKeeperRetriesControl retries_ctl("getAllFileInfos::getSizeAndChecksumForEachFileName", zookeeper_retries_info);
            retries_ctl.retryLoop([&]
            {
                auto zk = getZooKeeper();
                multi_response = zk->get(paths);
            });
        }

        for (size_t i = 0; i < batch.size(); ++i)
        {
            const String & escaped_file_name = batch[i];

            if (multi_response[i].error != Coordination::Error::ZOK)
                throw zkutil::KeeperException(multi_response[i].error);
            const auto & data = multi_response[i].data;

            FileInfo file_info;
            file_info.file_name = unescapeForFileName(escaped_file_name);
            auto size_and_checksum = deserializeSizeAndChecksum(data);
            file_info.size = size_and_checksum.first;
            file_info.checksum = size_and_checksum.second;
            file_infos.emplace_back(std::move(file_info));
        }
    }

    if (plain_backup)
    {
        /// If the backup is plain, `data_file_name` can be calculated easily and base backup isn't used.
        for (auto & file_info : file_infos)
            file_info.data_file_name = file_info.file_name;
    }
    else
    {
        /// If the backup isn't plain, a group of file infos can share the same `data_file_name`.

        /// Gets all non-empty sizes and checksums.
        Strings sizes_and_checksums;
        {
            ZooKeeperRetriesControl retries_ctl("getAllFileInfos::getAllSizesAndChecksums", zookeeper_retries_info);
            retries_ctl.retryLoop([&]()
            {
                auto zk = getZooKeeper();
                sizes_and_checksums = zk->getChildren(zookeeper_path + "/file_infos");
            });
        }

        /// For each non-empty size and checksum we read the name of a data file and the size and checksum of a corresponding file in the base backup.
        std::map<SizeAndChecksum, FileInfo> data_file_info_by_size_and_checksum;

        batches = split_vector(std::move(sizes_and_checksums), keeper_settings.batch_size_for_keeper_multiread);
        for (auto & batch : batches)
        {
            zkutil::ZooKeeper::MultiGetResponse multi_response;
            {
                Strings paths;
                paths.reserve(batch.size());
                for (const String & size_and_checksum : batch)
                    paths.emplace_back(zookeeper_path + "/file_infos/" + size_and_checksum);

                ZooKeeperRetriesControl retries_ctl("getAllFileInfos::getFileNameAndBaseInfoForEachSizeAndChecksum", zookeeper_retries_info);
                retries_ctl.retryLoop([&]
                {
                    auto zk = getZooKeeper();
                    multi_response = zk->get(paths);
                });
            }

            for (size_t i = 0; i < batch.size(); ++i)
            {
                auto size_and_checksum = deserializeSizeAndChecksum(batch[i]);

                if (multi_response[i].error != Coordination::Error::ZOK)
                    throw zkutil::KeeperException(multi_response[i].error);
                const auto & data = multi_response[i].data;

                FileInfo file_info;
                file_info.size = size_and_checksum.first;
                file_info.checksum = size_and_checksum.second;

                deserializeDataFileNameWithBaseInfo(data, file_info);

                data_file_info_by_size_and_checksum.emplace(size_and_checksum, std::move(file_info));
            }
        }

        /// Update the list of file infos with found names of data files. 
        for (auto & file_info : file_infos)
        {
            if (file_info.size)
            {
                auto it = data_file_info_by_size_and_checksum.find(std::make_pair(file_info.size, file_info.checksum));
                if (it != data_file_info_by_size_and_checksum.end())
                {
                    const auto & extra_info = it->second;
                    file_info.base_size = extra_info.base_size;
                    file_info.base_checksum = extra_info.base_checksum;
                    file_info.data_file_name = extra_info.data_file_name;
                }
            }
        }
    }

    /// Sort file infos alphabetically and check there are no duplicates.
    std::sort(file_infos.begin(), file_infos.end(), FileInfo::LessByFileName{});

    if (auto adjacent_it = std::adjacent_find(file_infos.begin(), file_infos.end(), FileInfo::EqualByFileName{});
        adjacent_it != file_infos.end())
    {
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_ALREADY_EXISTS, "Entry {} added multiple times to backup", quoteString(adjacent_it->file_name));
    }

    return file_infos;
}


bool BackupCoordinationRemote::hasConcurrentBackups(const std::atomic<size_t> &) const
{
    /// If its internal concurrency will be checked for the base backup
    if (is_internal)
        return false;

    auto zk = getZooKeeper();
    std::string backup_stage_path = zookeeper_path + "/stage";

    if (!zk->exists(root_zookeeper_path))
        zk->createAncestors(root_zookeeper_path);

    for (size_t attempt = 0; attempt < MAX_ZOOKEEPER_ATTEMPTS; ++attempt)
    {
        Coordination::Stat stat;
        zk->get(root_zookeeper_path, &stat);
        Strings existing_backup_paths = zk->getChildren(root_zookeeper_path);

        for (const auto & existing_backup_path : existing_backup_paths)
        {
            if (startsWith(existing_backup_path, "restore-"))
                continue;

            String existing_backup_uuid = existing_backup_path;
            existing_backup_uuid.erase(0, String("backup-").size());

            if (existing_backup_uuid == toString(backup_uuid))
                continue;

            const auto status = zk->get(root_zookeeper_path + "/" + existing_backup_path + "/stage");
            if (status != Stage::COMPLETED)
                return true;
        }

        zk->createIfNotExists(backup_stage_path, "");
        auto code = zk->trySet(backup_stage_path, Stage::SCHEDULED_TO_START, stat.version);
        if (code == Coordination::Error::ZOK)
            break;
        bool is_last_attempt = (attempt == MAX_ZOOKEEPER_ATTEMPTS - 1);
        if ((code != Coordination::Error::ZBADVERSION) || is_last_attempt)
            throw zkutil::KeeperException(code, backup_stage_path);
    }

    return false;
}

}
