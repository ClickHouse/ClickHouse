#include <Backups/BackupCoordinationRemote.h>

#include <base/hex.h>
#include <boost/algorithm/string/split.hpp>

#include <Access/Common/AccessEntityType.h>
#include <Backups/BackupCoordinationReplicatedAccess.h>
#include <Backups/BackupCoordinationStage.h>
#include <Common/ZooKeeper/Common.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <boost/algorithm/string.hpp>


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
                writeBinary(info.encrypted_by_disk, out);
                writeBinary(info.reference_target, out);
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
                readBinary(info.encrypted_by_disk, in);
                readBinary(info.reference_target, in);
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
    bool is_internal_,
    QueryStatusPtr process_list_element_)
    : root_zookeeper_path(root_zookeeper_path_)
    , zookeeper_path(root_zookeeper_path_ + "/backup-" + backup_uuid_)
    , keeper_settings(keeper_settings_)
    , backup_uuid(backup_uuid_)
    , all_hosts(all_hosts_)
    , current_host(current_host_)
    , current_host_index(findCurrentHostIndex(all_hosts, current_host))
    , plain_backup(plain_backup_)
    , is_internal(is_internal_)
    , log(getLogger("BackupCoordinationRemote"))
    , with_retries(
        log,
        get_zookeeper_,
        keeper_settings,
        process_list_element_,
        [my_zookeeper_path = zookeeper_path, my_current_host = current_host, my_is_internal = is_internal]
        (WithRetries::FaultyKeeper & zk)
        {
            /// Recreate this ephemeral node to signal that we are alive.
            if (my_is_internal)
            {
                String alive_node_path = my_zookeeper_path + "/stage/alive|" + my_current_host;

                /// Delete the ephemeral node from the previous connection so we don't have to wait for keeper to do it automatically.
                zk->tryRemove(alive_node_path);

                zk->createAncestors(alive_node_path);
                zk->create(alive_node_path, "", zkutil::CreateMode::Ephemeral);
            }
        })
{
    createRootNodes();

    stage_sync.emplace(
        zookeeper_path,
        with_retries,
        log);
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

void BackupCoordinationRemote::createRootNodes()
{
    auto holder = with_retries.createRetriesControlHolder("createRootNodes");
    holder.retries_ctl.retryLoop(
    [&, &zk = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zk);

        zk->createAncestors(zookeeper_path);

        Coordination::Requests ops;
        Coordination::Responses responses;
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/repl_part_names", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/repl_mutations", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/repl_data_paths", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/repl_access", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/repl_sql_objects", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/keeper_map_tables", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/file_infos", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/writing_files", "", zkutil::CreateMode::Persistent));
        zk->tryMulti(ops, responses);
    });
}

void BackupCoordinationRemote::removeAllNodes()
{
    auto holder = with_retries.createRetriesControlHolder("removeAllNodes");
    holder.retries_ctl.retryLoop(
    [&, &zk = holder.faulty_zookeeper]()
    {
        /// Usually this function is called by the initiator when a backup is complete so we don't need the coordination anymore.
        ///
        /// However there can be a rare situation when this function is called after an error occurs on the initiator of a query
        /// while some hosts are still making the backup. Removing all the nodes will remove the parent node of the backup coordination
        /// at `zookeeper_path` which might cause such hosts to stop with exception "ZNONODE". Or such hosts might still do some useless part
        /// of their backup work before that. Anyway in this case backup won't be finalized (because only an initiator can do that).
        with_retries.renewZooKeeper(zk);
        zk->removeRecursive(zookeeper_path);
    });
}


void BackupCoordinationRemote::setStage(const String & new_stage, const String & message)
{
    if (is_internal)
        stage_sync->set(current_host, new_stage, message);
    else
        stage_sync->set(current_host, new_stage, /* message */ "", /* all_hosts */ true);
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
        auto holder = with_retries.createRetriesControlHolder(logging_name + "::create");
        holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);
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

        auto holder = with_retries.createRetriesControlHolder(logging_name + "::createPart");
        holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);
            zk->createIfNotExists(part_path, part);
        });
    }
}

String BackupCoordinationRemote::deserializeFromMultipleZooKeeperNodes(const String & path, const String & logging_name) const
{
    Strings part_names;

    {
        auto holder = with_retries.createRetriesControlHolder(logging_name + "::getChildren");
        holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);
            part_names = zk->getChildren(path);
            std::sort(part_names.begin(), part_names.end());
        });
    }

    String res;
    for (const String & part_name : part_names)
    {
        String part;
        String part_path = path + "/" + part_name;
        auto holder = with_retries.createRetriesControlHolder(logging_name + "::get");
        holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);
            part = zk->get(part_path);
        });
        res += part;
    }
    return res;
}


void BackupCoordinationRemote::addReplicatedPartNames(
    const String & table_zk_path,
    const String & table_name_for_logs,
    const String & replica_name,
    const std::vector<PartNameAndChecksum> & part_names_and_checksums)
{
    {
        std::lock_guard lock{replicated_tables_mutex};
        if (replicated_tables)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedPartNames() must not be called after preparing");
    }

    auto holder = with_retries.createRetriesControlHolder("addReplicatedPartNames");
    holder.retries_ctl.retryLoop(
    [&, &zk = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zk);
        String path = zookeeper_path + "/repl_part_names/" + escapeForFileName(table_zk_path);
        zk->createIfNotExists(path, "");
        path += "/" + escapeForFileName(replica_name);
        zk->createIfNotExists(path, ReplicatedPartNames::serialize(part_names_and_checksums, table_name_for_logs));
    });
}

Strings BackupCoordinationRemote::getReplicatedPartNames(const String & table_zk_path, const String & replica_name) const
{
    std::lock_guard lock{replicated_tables_mutex};
    prepareReplicatedTables();
    return replicated_tables->getPartNames(table_zk_path, replica_name);
}

void BackupCoordinationRemote::addReplicatedMutations(
    const String & table_zk_path,
    const String & table_name_for_logs,
    const String & replica_name,
    const std::vector<MutationInfo> & mutations)
{
    {
        std::lock_guard lock{replicated_tables_mutex};
        if (replicated_tables)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedMutations() must not be called after preparing");
    }

    auto holder = with_retries.createRetriesControlHolder("addReplicatedMutations");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);
            String path = zookeeper_path + "/repl_mutations/" + escapeForFileName(table_zk_path);
            zk->createIfNotExists(path, "");
            path += "/" + escapeForFileName(replica_name);
            zk->createIfNotExists(path, ReplicatedMutations::serialize(mutations, table_name_for_logs));
        });
}

std::vector<IBackupCoordination::MutationInfo> BackupCoordinationRemote::getReplicatedMutations(const String & table_zk_path, const String & replica_name) const
{
    std::lock_guard lock{replicated_tables_mutex};
    prepareReplicatedTables();
    return replicated_tables->getMutations(table_zk_path, replica_name);
}


void BackupCoordinationRemote::addReplicatedDataPath(
    const String & table_zk_path, const String & data_path)
{
    {
        std::lock_guard lock{replicated_tables_mutex};
        if (replicated_tables)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedDataPath() must not be called after preparing");
    }

    auto holder = with_retries.createRetriesControlHolder("addReplicatedDataPath");
    holder.retries_ctl.retryLoop(
    [&, &zk = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zk);
        String path = zookeeper_path + "/repl_data_paths/" + escapeForFileName(table_zk_path);
        zk->createIfNotExists(path, "");
        path += "/" + escapeForFileName(data_path);
        zk->createIfNotExists(path, "");
    });
}

Strings BackupCoordinationRemote::getReplicatedDataPaths(const String & table_zk_path) const
{
    std::lock_guard lock{replicated_tables_mutex};
    prepareReplicatedTables();
    return replicated_tables->getDataPaths(table_zk_path);
}


void BackupCoordinationRemote::prepareReplicatedTables() const
{
    if (replicated_tables)
        return;

    std::vector<BackupCoordinationReplicatedTables::PartNamesForTableReplica> part_names_for_replicated_tables;
    {
        auto holder = with_retries.createRetriesControlHolder("prepareReplicatedTables::repl_part_names");
        holder.retries_ctl.retryLoop(
            [&, &zk = holder.faulty_zookeeper]()
        {
            part_names_for_replicated_tables.clear();
            with_retries.renewZooKeeper(zk);

            String path = zookeeper_path + "/repl_part_names";
            for (const String & escaped_table_zk_path : zk->getChildren(path))
            {
                String table_zk_path = unescapeForFileName(escaped_table_zk_path);
                String path2 = path + "/" + escaped_table_zk_path;
                for (const String & escaped_replica_name : zk->getChildren(path2))
                {
                    String replica_name = unescapeForFileName(escaped_replica_name);
                    auto part_names = ReplicatedPartNames::deserialize(zk->get(path2 + "/" + escaped_replica_name));
                    part_names_for_replicated_tables.push_back(
                        {table_zk_path, part_names.table_name_for_logs, replica_name, part_names.part_names_and_checksums});
                }
            }
        });
    }

    std::vector<BackupCoordinationReplicatedTables::MutationsForTableReplica> mutations_for_replicated_tables;
    {
        auto holder = with_retries.createRetriesControlHolder("prepareReplicatedTables::repl_mutations");
        holder.retries_ctl.retryLoop(
            [&, &zk = holder.faulty_zookeeper]()
        {
            mutations_for_replicated_tables.clear();
            with_retries.renewZooKeeper(zk);

            String path = zookeeper_path + "/repl_mutations";
            for (const String & escaped_table_zk_path : zk->getChildren(path))
            {
                String table_zk_path = unescapeForFileName(escaped_table_zk_path);
                String path2 = path + "/" + escaped_table_zk_path;
                for (const String & escaped_replica_name : zk->getChildren(path2))
                {
                    String replica_name = unescapeForFileName(escaped_replica_name);
                    auto mutations = ReplicatedMutations::deserialize(zk->get(path2 + "/" + escaped_replica_name));
                    mutations_for_replicated_tables.push_back(
                        {table_zk_path, mutations.table_name_for_logs, replica_name, mutations.mutations});
                }
            }
        });
    }

    std::vector<BackupCoordinationReplicatedTables::DataPathForTableReplica> data_paths_for_replicated_tables;
    {
        auto holder = with_retries.createRetriesControlHolder("prepareReplicatedTables::repl_data_paths");
        holder.retries_ctl.retryLoop(
            [&, &zk = holder.faulty_zookeeper]()
        {
            data_paths_for_replicated_tables.clear();
            with_retries.renewZooKeeper(zk);

            String path = zookeeper_path + "/repl_data_paths";
            for (const String & escaped_table_zk_path : zk->getChildren(path))
            {
                String table_zk_path = unescapeForFileName(escaped_table_zk_path);
                String path2 = path + "/" + escaped_table_zk_path;
                for (const String & escaped_data_path : zk->getChildren(path2))
                {
                    String data_path = unescapeForFileName(escaped_data_path);
                    data_paths_for_replicated_tables.push_back({table_zk_path, data_path});
                }
            }
        });
    }

    replicated_tables.emplace();
    for (auto & part_names : part_names_for_replicated_tables)
        replicated_tables->addPartNames(std::move(part_names));
    for (auto & mutations : mutations_for_replicated_tables)
        replicated_tables->addMutations(std::move(mutations));
    for (auto & data_paths : data_paths_for_replicated_tables)
        replicated_tables->addDataPath(std::move(data_paths));
}

void BackupCoordinationRemote::addReplicatedAccessFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & file_path)
{
    {
        std::lock_guard lock{replicated_access_mutex};
        if (replicated_access)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedAccessFilePath() must not be called after preparing");
    }

    auto holder = with_retries.createRetriesControlHolder("addReplicatedAccessFilePath");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zk);
        String path = zookeeper_path + "/repl_access/" + escapeForFileName(access_zk_path);
        zk->createIfNotExists(path, "");
        path += "/" + AccessEntityTypeInfo::get(access_entity_type).name;
        zk->createIfNotExists(path, "");
        path += "/" + current_host;
        zk->createIfNotExists(path, file_path);
    });
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

    std::vector<BackupCoordinationReplicatedAccess::FilePathForAccessEntity> file_path_for_access_entities;
    auto holder = with_retries.createRetriesControlHolder("prepareReplicatedAccess");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
    {
        file_path_for_access_entities.clear();
        with_retries.renewZooKeeper(zk);

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
                    file_path_for_access_entities.push_back({access_zk_path, type, host_id, file_path});
                }
            }
        }
    });

    replicated_access.emplace();
    for (auto & file_path : file_path_for_access_entities)
        replicated_access->addFilePath(std::move(file_path));
}

void BackupCoordinationRemote::addReplicatedSQLObjectsDir(const String & loader_zk_path, UserDefinedSQLObjectType object_type, const String & dir_path)
{
    {
        std::lock_guard lock{replicated_sql_objects_mutex};
        if (replicated_sql_objects)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedSQLObjectsDir() must not be called after preparing");
    }

    auto holder = with_retries.createRetriesControlHolder("addReplicatedSQLObjectsDir");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zk);
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
    });
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

    std::vector<BackupCoordinationReplicatedSQLObjects::DirectoryPathForSQLObject> directories_for_sql_objects;
    auto holder = with_retries.createRetriesControlHolder("prepareReplicatedSQLObjects");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
    {
        directories_for_sql_objects.clear();
        with_retries.renewZooKeeper(zk);

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
                    directories_for_sql_objects.push_back({loader_zk_path, object_type, host_id, dir});
                }
            }
        }
    });

    replicated_sql_objects.emplace();
    for (auto & directory : directories_for_sql_objects)
        replicated_sql_objects->addDirectory(std::move(directory));
}

void BackupCoordinationRemote::addKeeperMapTable(const String & table_zookeeper_root_path, const String & table_id, const String & data_path_in_backup)
{
    {
        std::lock_guard lock{keeper_map_tables_mutex};
        if (keeper_map_tables)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addKeeperMapTable() must not be called after preparing");
    }

    auto holder = with_retries.createRetriesControlHolder("addKeeperMapTable");
    holder.retries_ctl.retryLoop(
    [&, &zk = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zk);
        String path = zookeeper_path + "/keeper_map_tables/" + escapeForFileName(table_id);
        if (auto res
            = zk->tryCreate(path, fmt::format("{}\n{}", table_zookeeper_root_path, data_path_in_backup), zkutil::CreateMode::Persistent);
            res != Coordination::Error::ZOK && res != Coordination::Error::ZNODEEXISTS)
            throw zkutil::KeeperException(res);
    });
}

void BackupCoordinationRemote::prepareKeeperMapTables() const
{
    if (keeper_map_tables)
        return;

    std::vector<std::pair<std::string, BackupCoordinationKeeperMapTables::KeeperMapTableInfo>> keeper_map_table_infos;
    auto holder = with_retries.createRetriesControlHolder("prepareKeeperMapTables");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
    {
        keeper_map_table_infos.clear();

        with_retries.renewZooKeeper(zk);

        fs::path tables_path = fs::path(zookeeper_path) / "keeper_map_tables";

        auto tables = zk->getChildren(tables_path);
        keeper_map_table_infos.reserve(tables.size());

        for (auto & table : tables)
            table = tables_path / table;

        auto tables_info = zk->get(tables);
        for (size_t i = 0; i < tables_info.size(); ++i)
        {
            const auto & table_info = tables_info[i];

            if (table_info.error != Coordination::Error::ZOK)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Path in Keeper {} is unexpectedly missing", tables[i]);

            std::vector<std::string> data;
            boost::split(data, table_info.data, [](char c) { return c == '\n'; });
            keeper_map_table_infos.emplace_back(
                std::move(data[0]),
                BackupCoordinationKeeperMapTables::KeeperMapTableInfo{
                    .table_id = fs::path(tables[i]).filename(), .data_path_in_backup = std::move(data[1])});
        }
    });

    keeper_map_tables.emplace();
    for (const auto & [zk_root_path, table_info] : keeper_map_table_infos)
        keeper_map_tables->addTable(zk_root_path, table_info.table_id, table_info.data_path_in_backup);

}

String BackupCoordinationRemote::getKeeperMapDataPath(const String & table_zookeeper_root_path) const
{
    std::lock_guard lock(keeper_map_tables_mutex);
    prepareKeeperMapTables();
    return keeper_map_tables->getDataPath(table_zookeeper_root_path);
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
        auto holder = with_retries.createRetriesControlHolder("prepareFileInfos::get_hosts");
        holder.retries_ctl.retryLoop(
            [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);
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
    {
        /// Check if this host is already writing this file.
        std::lock_guard lock{writing_files_mutex};
        if (writing_files.contains(data_file_index))
            return false;
    }

    /// Store in Zookeeper that this host is the only host which is allowed to write this file.
    bool host_is_assigned = false;
    String full_path = zookeeper_path + "/writing_files/" + std::to_string(data_file_index);
    String host_index_str = std::to_string(current_host_index);

    auto holder = with_retries.createRetriesControlHolder("startWritingFile");
    holder.retries_ctl.retryLoop(
            [&, &zk = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zk);
        auto code = zk->tryCreate(full_path, host_index_str, zkutil::CreateMode::Persistent);

        if (code == Coordination::Error::ZOK)
            host_is_assigned = true; /// If we've just created this ZooKeeper's node, this host is assigned.
        else if (code == Coordination::Error::ZNODEEXISTS)
            host_is_assigned = (zk->get(full_path) == host_index_str); /// The previous retry could write this ZooKeeper's node and then fail.
        else
            throw zkutil::KeeperException::fromPath(code, full_path);
    });

    if (!host_is_assigned)
        return false; /// Other host is writing this file.

    {
        /// Check if this host is already writing this file,
        /// and if it's not, mark that this host is writing this file.
        /// We have to check that again because we were accessing ZooKeeper with the mutex unlocked.
        std::lock_guard lock{writing_files_mutex};
        return writing_files.emplace(data_file_index).second; /// Return false if this host is already writing this file.
    }
}

bool BackupCoordinationRemote::hasConcurrentBackups(const std::atomic<size_t> &) const
{
    /// If its internal concurrency will be checked for the base backup
    if (is_internal)
        return false;

    std::string backup_stage_path = zookeeper_path + "/stage";

    bool result = false;

    auto holder = with_retries.createRetriesControlHolder("getAllArchiveSuffixes");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zk);

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

                String status;
                if (zk->tryGet(root_zookeeper_path + "/" + existing_backup_path + "/stage", status))
                {
                    /// Check if some other backup is in progress
                    if (status == Stage::SCHEDULED_TO_START)
                    {
                        LOG_WARNING(log, "Found a concurrent backup: {}, current backup: {}", existing_backup_uuid, toString(backup_uuid));
                        result = true;
                        return;
                    }
                }
            }

            zk->createIfNotExists(backup_stage_path, "");
            auto code = zk->trySet(backup_stage_path, Stage::SCHEDULED_TO_START, stat.version);
            if (code == Coordination::Error::ZOK)
                break;
            bool is_last_attempt = (attempt == MAX_ZOOKEEPER_ATTEMPTS - 1);
            if ((code != Coordination::Error::ZBADVERSION) || is_last_attempt)
                throw zkutil::KeeperException::fromPath(code, backup_stage_path);
        }
    });

    return result;
}

}
