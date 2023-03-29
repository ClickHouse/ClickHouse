#include <Backups/BackupCoordinationRemote.h>
#include <Access/Common/AccessEntityType.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>
#include <base/hex.h>
#include <Backups/BackupCoordinationStage.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_NODE_IN_ZOOKEEPER;
    extern const int LOGICAL_ERROR;
}

namespace Stage = BackupCoordinationStage;

/// zookeeper_path/file_names/file_name->checksum_and_size
/// zookeeper_path/file_infos/checksum_and_size->info
/// zookeeper_path/archive_suffixes
/// zookeeper_path/current_archive_suffix

namespace
{
    using SizeAndChecksum = IBackupCoordination::SizeAndChecksum;
    using FileInfo = IBackupCoordination::FileInfo;
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

    String serializeFileInfo(const FileInfo & info)
    {
        WriteBufferFromOwnString out;
        writeBinary(info.file_name, out);
        writeBinary(info.size, out);
        writeBinary(info.checksum, out);
        writeBinary(info.base_size, out);
        writeBinary(info.base_checksum, out);
        writeBinary(info.data_file_name, out);
        writeBinary(info.archive_suffix, out);
        writeBinary(info.pos_in_archive, out);
        return out.str();
    }

    FileInfo deserializeFileInfo(const String & str)
    {
        FileInfo info;
        ReadBufferFromString in{str};
        readBinary(info.file_name, in);
        readBinary(info.size, in);
        readBinary(info.checksum, in);
        readBinary(info.base_size, in);
        readBinary(info.base_checksum, in);
        readBinary(info.data_file_name, in);
        readBinary(info.archive_suffix, in);
        readBinary(info.pos_in_archive, in);
        return info;
    }

    String serializeSizeAndChecksum(const SizeAndChecksum & size_and_checksum)
    {
        return getHexUIntLowercase(size_and_checksum.second) + '_' + std::to_string(size_and_checksum.first);
    }

    SizeAndChecksum deserializeSizeAndChecksum(const String & str)
    {
        constexpr size_t num_chars_in_checksum = sizeof(UInt128) * 2;
        if (str.size() <= num_chars_in_checksum)
            throw Exception(
                ErrorCodes::UNEXPECTED_NODE_IN_ZOOKEEPER,
                "Unexpected size of checksum: {}, must be {}",
                str.size(),
                num_chars_in_checksum);
        UInt128 checksum = unhexUInt<UInt128>(str.data());
        UInt64 size = parseFromString<UInt64>(str.substr(num_chars_in_checksum + 1));
        return std::pair{size, checksum};
    }

    size_t extractCounterFromSequentialNodeName(const String & node_name)
    {
        size_t pos_before_counter = node_name.find_last_not_of("0123456789");
        size_t counter_length = node_name.length() - 1 - pos_before_counter;
        auto counter = std::string_view{node_name}.substr(node_name.length() - counter_length);
        return parseFromString<UInt64>(counter);
    }

    String formatArchiveSuffix(size_t counter)
    {
        return fmt::format("{:03}", counter); /// Outputs 001, 002, 003, ...
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
    bool is_internal_)
    : get_zookeeper(get_zookeeper_)
    , root_zookeeper_path(root_zookeeper_path_)
    , zookeeper_path(root_zookeeper_path_ + "/backup-" + backup_uuid_)
    , keeper_settings(keeper_settings_)
    , backup_uuid(backup_uuid_)
    , all_hosts(all_hosts_)
    , current_host(current_host_)
    , current_host_index(findCurrentHostIndex(all_hosts, current_host))
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
    zk->createIfNotExists(zookeeper_path + "/archive_suffixes", "");
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
    auto zk = getZooKeeper();

    String full_path = zookeeper_path + "/file_names/" + escapeForFileName(file_info.file_name);
    String size_and_checksum = serializeSizeAndChecksum(std::pair{file_info.size, file_info.checksum});
    zk->create(full_path, size_and_checksum, zkutil::CreateMode::Persistent);

    if (!file_info.size)
    {
        is_data_file_required = false;
        return;
    }

    full_path = zookeeper_path + "/file_infos/" + size_and_checksum;
    auto code = zk->tryCreate(full_path, serializeFileInfo(file_info), zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, full_path);

    is_data_file_required = (code == Coordination::Error::ZOK) && (file_info.size > file_info.base_size);
}

void BackupCoordinationRemote::updateFileInfo(const FileInfo & file_info)
{
    if (!file_info.size)
        return; /// we don't keep FileInfos for empty files, nothing to update

    auto zk = getZooKeeper();
    String size_and_checksum = serializeSizeAndChecksum(std::pair{file_info.size, file_info.checksum});
    String full_path = zookeeper_path + "/file_infos/" + size_and_checksum;
    for (size_t attempt = 0; attempt < MAX_ZOOKEEPER_ATTEMPTS; ++attempt)
    {
        Coordination::Stat stat;
        auto new_info = deserializeFileInfo(zk->get(full_path, &stat));
        new_info.archive_suffix = file_info.archive_suffix;
        auto code = zk->trySet(full_path, serializeFileInfo(new_info), stat.version);
        if (code == Coordination::Error::ZOK)
            return;
        bool is_last_attempt = (attempt == MAX_ZOOKEEPER_ATTEMPTS - 1);
        if ((code != Coordination::Error::ZBADVERSION) || is_last_attempt)
            throw zkutil::KeeperException(code, full_path);
    }
}

std::vector<FileInfo> BackupCoordinationRemote::getAllFileInfos() const
{
    /// There could be tons of files inside /file_names or /file_infos
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

    std::vector<Strings> batched_escaped_names;
    {
        ZooKeeperRetriesControl retries_ctl("getAllFileInfos::getChildren", zookeeper_retries_info);
        retries_ctl.retryLoop([&]()
        {
            auto zk = getZooKeeper();
            batched_escaped_names = split_vector(zk->getChildren(zookeeper_path + "/file_names"), keeper_settings.batch_size_for_keeper_multiread);
        });
    }

    std::vector<FileInfo> file_infos;
    file_infos.reserve(batched_escaped_names.size());

    for (auto & batch : batched_escaped_names)
    {
        zkutil::ZooKeeper::MultiGetResponse sizes_and_checksums;
        {
            Strings file_names_paths;
            file_names_paths.reserve(batch.size());
            for (const String & escaped_name : batch)
                file_names_paths.emplace_back(zookeeper_path + "/file_names/" + escaped_name);


            ZooKeeperRetriesControl retries_ctl("getAllFileInfos::getSizesAndChecksums", zookeeper_retries_info);
            retries_ctl.retryLoop([&]
            {
                auto zk = getZooKeeper();
                sizes_and_checksums = zk->get(file_names_paths);
            });
        }

        Strings non_empty_file_names;
        Strings non_empty_file_infos_paths;
        std::vector<FileInfo> non_empty_files_infos;

        /// Process all files and understand whether there are some empty files
        /// Save non empty file names for further batch processing
        {
            std::vector<FileInfo> empty_files_infos;
            for (size_t i = 0; i < batch.size(); ++i)
            {
                auto file_name = batch[i];
                if (sizes_and_checksums[i].error != Coordination::Error::ZOK)
                    throw zkutil::KeeperException(sizes_and_checksums[i].error);
                const auto & size_and_checksum = sizes_and_checksums[i].data;
                auto size = deserializeSizeAndChecksum(size_and_checksum).first;

                if (size)
                {
                    /// Save it later for batch processing
                    non_empty_file_names.emplace_back(file_name);
                    non_empty_file_infos_paths.emplace_back(zookeeper_path + "/file_infos/" + size_and_checksum);
                    continue;
                }

                /// File is empty
                FileInfo empty_file_info;
                empty_file_info.file_name = unescapeForFileName(file_name);
                empty_files_infos.emplace_back(std::move(empty_file_info));
            }

            std::move(empty_files_infos.begin(), empty_files_infos.end(), std::back_inserter(file_infos));
        }

        zkutil::ZooKeeper::MultiGetResponse non_empty_file_infos_serialized;
        ZooKeeperRetriesControl retries_ctl("getAllFileInfos::getFileInfos", zookeeper_retries_info);
        retries_ctl.retryLoop([&]()
        {
            auto zk = getZooKeeper();
            non_empty_file_infos_serialized = zk->get(non_empty_file_infos_paths);
        });

        /// Process non empty files
        for (size_t i = 0; i < non_empty_file_names.size(); ++i)
        {
            FileInfo file_info;
            if (non_empty_file_infos_serialized[i].error != Coordination::Error::ZOK)
                throw zkutil::KeeperException(non_empty_file_infos_serialized[i].error);
            file_info = deserializeFileInfo(non_empty_file_infos_serialized[i].data);
            file_info.file_name = unescapeForFileName(non_empty_file_names[i]);
            non_empty_files_infos.emplace_back(std::move(file_info));
        }

        std::move(non_empty_files_infos.begin(), non_empty_files_infos.end(), std::back_inserter(file_infos));
    }

    return file_infos;
}

Strings BackupCoordinationRemote::listFiles(const String & directory, bool recursive) const
{
    auto zk = getZooKeeper();
    Strings escaped_names = zk->getChildren(zookeeper_path + "/file_names");

    String prefix = directory;
    if (!prefix.empty() && !prefix.ends_with('/'))
        prefix += '/';
    String terminator = recursive ? "" : "/";

    Strings elements;
    std::unordered_set<std::string_view> unique_elements;

    for (const String & escaped_name : escaped_names)
    {
        String name = unescapeForFileName(escaped_name);
        if (!name.starts_with(prefix))
            continue;
        size_t start_pos = prefix.length();
        size_t end_pos = String::npos;
        if (!terminator.empty())
            end_pos = name.find(terminator, start_pos);
        std::string_view new_element = std::string_view{name}.substr(start_pos, end_pos - start_pos);
        if (unique_elements.contains(new_element))
            continue;
        elements.push_back(String{new_element});
        unique_elements.emplace(new_element);
    }

    ::sort(elements.begin(), elements.end());
    return elements;
}

bool BackupCoordinationRemote::hasFiles(const String & directory) const
{
    auto zk = getZooKeeper();
    Strings escaped_names = zk->getChildren(zookeeper_path + "/file_names");

    String prefix = directory;
    if (!prefix.empty() && !prefix.ends_with('/'))
        prefix += '/';

    for (const String & escaped_name : escaped_names)
    {
        String name = unescapeForFileName(escaped_name);
        if (name.starts_with(prefix))
            return true;
    }

    return false;
}

std::optional<FileInfo> BackupCoordinationRemote::getFileInfo(const String & file_name) const
{
    auto zk = getZooKeeper();
    String size_and_checksum;
    if (!zk->tryGet(zookeeper_path + "/file_names/" + escapeForFileName(file_name), size_and_checksum))
        return std::nullopt;
    UInt64 size = deserializeSizeAndChecksum(size_and_checksum).first;
    FileInfo file_info;
    if (size) /// we don't keep FileInfos for empty files
        file_info = deserializeFileInfo(zk->get(zookeeper_path + "/file_infos/" + size_and_checksum));
    file_info.file_name = file_name;
    return file_info;
}

std::optional<FileInfo> BackupCoordinationRemote::getFileInfo(const SizeAndChecksum & size_and_checksum) const
{
    auto zk = getZooKeeper();
    String file_info_str;
    if (!zk->tryGet(zookeeper_path + "/file_infos/" + serializeSizeAndChecksum(size_and_checksum), file_info_str))
        return std::nullopt;
    return deserializeFileInfo(file_info_str);
}

String BackupCoordinationRemote::getNextArchiveSuffix()
{
    auto zk = getZooKeeper();
    String path = zookeeper_path + "/archive_suffixes/a";
    String path_created;
    auto code = zk->tryCreate(path, "", zkutil::CreateMode::PersistentSequential, path_created);
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException(code, path);
    return formatArchiveSuffix(extractCounterFromSequentialNodeName(path_created));
}

Strings BackupCoordinationRemote::getAllArchiveSuffixes() const
{
    auto zk = getZooKeeper();
    Strings node_names = zk->getChildren(zookeeper_path + "/archive_suffixes");
    for (auto & node_name : node_names)
        node_name = formatArchiveSuffix(extractCounterFromSequentialNodeName(node_name));
    return node_names;
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
