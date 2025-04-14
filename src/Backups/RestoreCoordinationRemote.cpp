#include <Backups/BackupCoordinationRemote.h>
#include <Backups/BackupCoordinationStage.h>
#include <Backups/RestoreCoordinationRemote.h>
#include <Backups/BackupCoordinationStageSync.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/CreateQueryUUIDs.h>
#include <Parsers/formatAST.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>


namespace DB
{

namespace Stage = BackupCoordinationStage;

RestoreCoordinationRemote::RestoreCoordinationRemote(
    zkutil::GetZooKeeper get_zookeeper_,
    const String & root_zookeeper_path_,
    const RestoreKeeperSettings & keeper_settings_,
    const String & restore_uuid_,
    const Strings & all_hosts_,
    const String & current_host_,
    bool is_internal_,
    QueryStatusPtr process_list_element_)
    : get_zookeeper(get_zookeeper_)
    , root_zookeeper_path(root_zookeeper_path_)
    , keeper_settings(keeper_settings_)
    , restore_uuid(restore_uuid_)
    , zookeeper_path(root_zookeeper_path_ + "/restore-" + restore_uuid_)
    , all_hosts(all_hosts_)
    , current_host(current_host_)
    , current_host_index(BackupCoordinationRemote::findCurrentHostIndex(all_hosts, current_host))
    , is_internal(is_internal_)
    , log(getLogger("RestoreCoordinationRemote"))
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

RestoreCoordinationRemote::~RestoreCoordinationRemote()
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

void RestoreCoordinationRemote::createRootNodes()
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
            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/repl_databases_tables_acquired", "", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/repl_tables_data_acquired", "", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/repl_access_storages_acquired", "", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/repl_sql_objects_acquired", "", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/keeper_map_tables", "", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/table_uuids", "", zkutil::CreateMode::Persistent));
            zk->tryMulti(ops, responses);
        });
}

void RestoreCoordinationRemote::setStage(const String & new_stage, const String & message)
{
    if (is_internal)
        stage_sync->set(current_host, new_stage, message);
    else
        stage_sync->set(current_host, new_stage, /* message */ "", /* all_hosts */ true);
}

void RestoreCoordinationRemote::setError(const Exception & exception)
{
    stage_sync->setError(current_host, exception);
}

Strings RestoreCoordinationRemote::waitForStage(const String & stage_to_wait)
{
    return stage_sync->wait(all_hosts, stage_to_wait);
}

Strings RestoreCoordinationRemote::waitForStage(const String & stage_to_wait, std::chrono::milliseconds timeout)
{
    return stage_sync->waitFor(all_hosts, stage_to_wait, timeout);
}

bool RestoreCoordinationRemote::acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name)
{
    bool result = false;
    auto holder = with_retries.createRetriesControlHolder("acquireCreatingTableInReplicatedDatabase");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            String path = zookeeper_path + "/repl_databases_tables_acquired/" + escapeForFileName(database_zk_path);
            zk->createIfNotExists(path, "");

            path += "/" + escapeForFileName(table_name);
            auto code = zk->tryCreate(path, toString(current_host_index), zkutil::CreateMode::Persistent);
            if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
                throw zkutil::KeeperException::fromPath(code, path);

            if (code == Coordination::Error::ZOK)
            {
                result = true;
                return;
            }

            /// We need to check who created that node
            result = zk->get(path) == toString(current_host_index);
        });
    return result;
}

bool RestoreCoordinationRemote::acquireInsertingDataIntoReplicatedTable(const String & table_zk_path)
{
    bool result = false;
    auto holder = with_retries.createRetriesControlHolder("acquireInsertingDataIntoReplicatedTable");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            String path = zookeeper_path + "/repl_tables_data_acquired/" + escapeForFileName(table_zk_path);
            auto code = zk->tryCreate(path, toString(current_host_index), zkutil::CreateMode::Persistent);
            if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
                throw zkutil::KeeperException::fromPath(code, path);

            if (code == Coordination::Error::ZOK)
            {
                result = true;
                return;
            }

            /// We need to check who created that node
            result = zk->get(path) == toString(current_host_index);
        });
    return result;
}

bool RestoreCoordinationRemote::acquireReplicatedAccessStorage(const String & access_storage_zk_path)
{
    bool result = false;
    auto holder = with_retries.createRetriesControlHolder("acquireReplicatedAccessStorage");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            String path = zookeeper_path + "/repl_access_storages_acquired/" + escapeForFileName(access_storage_zk_path);
            auto code = zk->tryCreate(path, toString(current_host_index), zkutil::CreateMode::Persistent);
            if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
                throw zkutil::KeeperException::fromPath(code, path);

            if (code == Coordination::Error::ZOK)
            {
                result = true;
                return;
            }

            /// We need to check who created that node
            result = zk->get(path) == toString(current_host_index);
        });
    return result;
}

bool RestoreCoordinationRemote::acquireReplicatedSQLObjects(const String & loader_zk_path, UserDefinedSQLObjectType object_type)
{
    bool result = false;
    auto holder = with_retries.createRetriesControlHolder("acquireReplicatedSQLObjects");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            String path = zookeeper_path + "/repl_sql_objects_acquired/" + escapeForFileName(loader_zk_path);
            zk->createIfNotExists(path, "");

            path += "/";
            switch (object_type)
            {
                case UserDefinedSQLObjectType::Function:
                    path += "functions";
                    break;
            }

            auto code = zk->tryCreate(path, "", zkutil::CreateMode::Persistent);
            if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
                throw zkutil::KeeperException::fromPath(code, path);

            if (code == Coordination::Error::ZOK)
            {
                result = true;
                return;
            }

            /// We need to check who created that node
            result =  zk->get(path) == toString(current_host_index);
        });
    return result;
}

bool RestoreCoordinationRemote::acquireInsertingDataForKeeperMap(const String & root_zk_path, const String & table_unique_id)
{
    bool lock_acquired = false;
    auto holder = with_retries.createRetriesControlHolder("acquireInsertingDataForKeeperMap");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            /// we need to remove leading '/' from root_zk_path
            auto normalized_root_zk_path = root_zk_path.substr(1);
            std::string restore_lock_path = fs::path(zookeeper_path) / "keeper_map_tables" / escapeForFileName(normalized_root_zk_path);
            zk->createAncestors(restore_lock_path);
            auto code = zk->tryCreate(restore_lock_path, table_unique_id, zkutil::CreateMode::Persistent);

            if (code == Coordination::Error::ZOK)
            {
                lock_acquired = true;
                return;
            }

            if (code == Coordination::Error::ZNODEEXISTS)
                lock_acquired = table_unique_id == zk->get(restore_lock_path);
            else
                zkutil::KeeperException::fromPath(code, restore_lock_path);
        });
    return lock_acquired;
}

void RestoreCoordinationRemote::generateUUIDForTable(ASTCreateQuery & create_query)
{
    String query_str = serializeAST(create_query);
    CreateQueryUUIDs new_uuids{create_query, /* generate_random= */ true, /* force_random= */ true};
    String new_uuids_str = new_uuids.toString();

    auto holder = with_retries.createRetriesControlHolder("generateUUIDForTable");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            String path = zookeeper_path + "/table_uuids/" + escapeForFileName(query_str);
            Coordination::Error res = zk->tryCreate(path, new_uuids_str, zkutil::CreateMode::Persistent);

            if (res == Coordination::Error::ZOK)
            {
                new_uuids.copyToQuery(create_query);
                return;
            }

            if (res == Coordination::Error::ZNODEEXISTS)
            {
                CreateQueryUUIDs::fromString(zk->get(path)).copyToQuery(create_query);
                return;
            }

            zkutil::KeeperException::fromPath(res, path);
        });
}

void RestoreCoordinationRemote::removeAllNodes()
{
    /// Usually this function is called by the initiator when a restore operation is complete so we don't need the coordination anymore.
    ///
    /// However there can be a rare situation when this function is called after an error occurs on the initiator of a query
    /// while some hosts are still restoring something. Removing all the nodes will remove the parent node of the restore coordination
    /// at `zookeeper_path` which might cause such hosts to stop with exception "ZNONODE". Or such hosts might still do some part
    /// of their restore work before that.

    auto holder = with_retries.createRetriesControlHolder("removeAllNodes");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);
            zk->removeRecursive(zookeeper_path);
        });
}

bool RestoreCoordinationRemote::hasConcurrentRestores(const std::atomic<size_t> &) const
{
    /// If its internal concurrency will be checked for the base restore
    if (is_internal)
        return false;

    bool result = false;
    std::string path = zookeeper_path + "/stage";

    auto holder = with_retries.createRetriesControlHolder("createRootNodes");
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            if (! zk->exists(root_zookeeper_path))
                zk->createAncestors(root_zookeeper_path);

            for (size_t attempt = 0; attempt < MAX_ZOOKEEPER_ATTEMPTS; ++attempt)
            {
                Coordination::Stat stat;
                zk->get(root_zookeeper_path, &stat);
                Strings existing_restore_paths = zk->getChildren(root_zookeeper_path);
                for (const auto & existing_restore_path : existing_restore_paths)
                {
                    if (startsWith(existing_restore_path, "backup-"))
                        continue;

                    String existing_restore_uuid = existing_restore_path;
                    existing_restore_uuid.erase(0, String("restore-").size());

                    if (existing_restore_uuid == toString(restore_uuid))
                        continue;

                    String status;
                    if (zk->tryGet(root_zookeeper_path + "/" + existing_restore_path + "/stage", status))
                    {
                        /// Check if some other restore is in progress
                        if (status == Stage::SCHEDULED_TO_START)
                        {
                            LOG_WARNING(log, "Found a concurrent restore: {}, current restore: {}", existing_restore_uuid, toString(restore_uuid));
                            result = true;
                            return;
                        }
                    }
                }

                zk->createIfNotExists(path, "");
                auto code = zk->trySet(path, Stage::SCHEDULED_TO_START, stat.version);
                if (code == Coordination::Error::ZOK)
                    break;
                bool is_last_attempt = (attempt == MAX_ZOOKEEPER_ATTEMPTS - 1);
                if ((code != Coordination::Error::ZBADVERSION) || is_last_attempt)
                    throw zkutil::KeeperException::fromPath(code, path);
            }
        });

    return result;
}

}
