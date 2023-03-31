#include <Backups/BackupCoordinationRemote.h>
#include <Access/Common/AccessEntityType.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>
#include <Backups/BackupCoordinationStage.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Stage = BackupCoordinationStage;

namespace
{
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

    struct FileInfos
    {
        BackupFileInfos file_infos;

        static String serialize(const BackupFileInfos & file_infos_)
        {
            WriteBufferFromOwnString out;
            writeBinary(file_infos_.size(), out);
            for (const auto & info : file_infos_)
            {
                writeBinary(info.file_name, out);
                writeBinary(info.size, out);
                writeBinary(info.checksum, out);
                writeBinary(info.base_size, out);
                writeBinary(info.base_checksum, out);
                /// We don't store `info.data_file_name` and `info.data_file_index` because they're determined automalically
                /// after reading file infos for all the hosts (see the class BackupCoordinationFileInfos).
            }
            return out.str();
        }

        static FileInfos deserialize(const String & str)
        {
            ReadBufferFromString in{str};
            FileInfos res;
            size_t num;
            readBinary(num, in);
            res.file_infos.resize(num);
            for (size_t i = 0; i != num; ++i)
            {
                auto & info = res.file_infos[i];
                readBinary(info.file_name, in);
                readBinary(info.size, in);
                readBinary(info.checksum, in);
                readBinary(info.base_size, in);
                readBinary(info.base_checksum, in);
            }
            return res;
        }
    };
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
    std::lock_guard lock{zookeeper_mutex};
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
    zk->createIfNotExists(zookeeper_path + "/file_infos", "");
    zk->createIfNotExists(zookeeper_path + "/writing_files", "");
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


void BackupCoordinationRemote::serializeToMultipleZooKeeperNodes(const String & path, const String & value, const String & logging_name)
{
    {
        ZooKeeperRetriesControl retries_ctl(logging_name + "::create", zookeeper_retries_info);
        retries_ctl.retryLoop([&]
        {
            auto zk = getZooKeeper();
            zk->createIfNotExists(path, "");
        });
    }

    if (value.empty())
        return;

    size_t max_part_size = keeper_settings.keeper_value_max_size;
    if (!max_part_size)
        max_part_size = value.size();

    size_t num_parts = (value.size() + max_part_size - 1) / max_part_size; /// round up

    for (size_t i = 0; i != num_parts; ++i)
    {
        size_t begin = i * max_part_size;
        size_t end = std::min(begin + max_part_size, value.size());
        String part = value.substr(begin, end - begin);
        String part_path = fmt::format("{}/{:06}", path, i);

        ZooKeeperRetriesControl retries_ctl(logging_name + "::createPart", zookeeper_retries_info);
        retries_ctl.retryLoop([&]
        {
            auto zk = getZooKeeper();
            zk->createIfNotExists(part_path, part);
        });
    }
}

String BackupCoordinationRemote::deserializeFromMultipleZooKeeperNodes(const String & path, const String & logging_name) const
{
    Strings part_names;

    {
        ZooKeeperRetriesControl retries_ctl(logging_name + "::getChildren", zookeeper_retries_info);
        retries_ctl.retryLoop([&]{
            auto zk = getZooKeeper();
            part_names = zk->getChildren(path);
            std::sort(part_names.begin(), part_names.end());
        });
    }

    String res;
    for (const String & part_name : part_names)
    {
        String part;
        String part_path = path + "/" + part_name;
        ZooKeeperRetriesControl retries_ctl(logging_name + "::get", zookeeper_retries_info);
        retries_ctl.retryLoop([&]
        {
            auto zk = getZooKeeper();
            part = zk->get(part_path);
        });
        res += part;
    }
    return res;
}


void BackupCoordinationRemote::addReplicatedPartNames(
    const String & table_shared_id,
    const String & table_name_for_logs,
    const String & replica_name,
    const std::vector<PartNameAndChecksum> & part_names_and_checksums)
{
    {
        std::lock_guard lock{replicated_tables_mutex};
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
    std::lock_guard lock{replicated_tables_mutex};
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
        std::lock_guard lock{replicated_tables_mutex};
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
    std::lock_guard lock{replicated_tables_mutex};
    prepareReplicatedTables();
    return replicated_tables->getMutations(table_shared_id, replica_name);
}


void BackupCoordinationRemote::addReplicatedDataPath(
    const String & table_shared_id, const String & data_path)
{
    {
        std::lock_guard lock{replicated_tables_mutex};
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
    std::lock_guard lock{replicated_tables_mutex};
    prepareReplicatedTables();
    return replicated_tables->getDataPaths(table_shared_id);
}


void BackupCoordinationRemote::prepareReplicatedTables() const
{
    if (replicated_tables)
        return;

    replicated_tables.emplace();
    auto zk = getZooKeeper();

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
        std::lock_guard lock{replicated_access_mutex};
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
    std::lock_guard lock{replicated_access_mutex};
    prepareReplicatedAccess();
    return replicated_access->getFilePaths(access_zk_path, access_entity_type, current_host);
}

void BackupCoordinationRemote::prepareReplicatedAccess() const
{
    if (replicated_access)
        return;

    replicated_access.emplace();
    auto zk = getZooKeeper();

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
        std::lock_guard lock{replicated_sql_objects_mutex};
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
    std::lock_guard lock{replicated_sql_objects_mutex};
    prepareReplicatedSQLObjects();
    return replicated_sql_objects->getDirectories(loader_zk_path, object_type, current_host);
}

void BackupCoordinationRemote::prepareReplicatedSQLObjects() const
{
    if (replicated_sql_objects)
        return;

    replicated_sql_objects.emplace();
    auto zk = getZooKeeper();

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


void BackupCoordinationRemote::addFileInfos(BackupFileInfos && file_infos_)
{
    {
        std::lock_guard lock{file_infos_mutex};
        if (file_infos)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addFileInfos() must not be called after preparing");
    }

    /// Serialize `file_infos_` and write it to ZooKeeper's nodes.
    String file_infos_str = FileInfos::serialize(file_infos_);
    serializeToMultipleZooKeeperNodes(zookeeper_path + "/file_infos/" + current_host, file_infos_str, "addFileInfos");
}

BackupFileInfos BackupCoordinationRemote::getFileInfos() const
{
    std::lock_guard lock{file_infos_mutex};
    prepareFileInfos();
    return file_infos->getFileInfos(current_host);
}

BackupFileInfos BackupCoordinationRemote::getFileInfosForAllHosts() const
{
    std::lock_guard lock{file_infos_mutex};
    prepareFileInfos();
    return file_infos->getFileInfosForAllHosts();
}

void BackupCoordinationRemote::prepareFileInfos() const
{
    if (file_infos)
        return;

    file_infos.emplace(plain_backup);

    Strings hosts_with_file_infos;
    {
        ZooKeeperRetriesControl retries_ctl("prepareFileInfos::get_hosts", zookeeper_retries_info);
        retries_ctl.retryLoop([&]{
            auto zk = getZooKeeper();
            hosts_with_file_infos = zk->getChildren(zookeeper_path + "/file_infos");
        });
    }

    for (const String & host : hosts_with_file_infos)
    {
        String file_infos_str = deserializeFromMultipleZooKeeperNodes(zookeeper_path + "/file_infos/" + host, "prepareFileInfos");
        auto deserialized_file_infos = FileInfos::deserialize(file_infos_str).file_infos;
        file_infos->addFileInfos(std::move(deserialized_file_infos), host);
    }
}

bool BackupCoordinationRemote::startWritingFile(size_t data_file_index)
{
    bool acquired_writing = false;
    String full_path = zookeeper_path + "/writing_files/" + std::to_string(data_file_index);
    String host_index_str = std::to_string(current_host_index);

    ZooKeeperRetriesControl retries_ctl("startWritingFile", zookeeper_retries_info);
    retries_ctl.retryLoop([&]
    {
        auto zk = getZooKeeper();
        auto code = zk->tryCreate(full_path, host_index_str, zkutil::CreateMode::Persistent);

        if (code == Coordination::Error::ZOK)
            acquired_writing = true; /// If we've just created this ZooKeeper's node, the writing is acquired, i.e. we should write this data file.
        else if (code == Coordination::Error::ZNODEEXISTS)
            acquired_writing = (zk->get(full_path) == host_index_str); /// The previous retry could write this ZooKeeper's node and then fail.
        else
            throw zkutil::KeeperException(code, full_path);
    });

    return acquired_writing;
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
