#include <Backups/BackupCoordinationOnCluster.h>

#include <Backups/BackupCoordinationStage.h>
#include <Backups/BackupCoordinationStageSync.h>
#include <Backups/RestoreCoordinationOnCluster.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/CreateQueryUUIDs.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>


namespace DB
{

RestoreCoordinationOnCluster::RestoreCoordinationOnCluster(
    const UUID & restore_uuid_,
    const String & root_zookeeper_path_,
    zkutil::GetZooKeeper get_zookeeper_,
    const BackupKeeperSettings & keeper_settings_,
    const String & current_host_,
    const Strings & all_hosts_,
    bool allow_concurrent_restore_,
    BackupConcurrencyCounters & concurrency_counters_,
    ThreadPoolCallbackRunnerUnsafe<void> schedule_,
    QueryStatusPtr process_list_element_)
    : root_zookeeper_path(root_zookeeper_path_)
    , keeper_settings(keeper_settings_)
    , restore_uuid(restore_uuid_)
    , zookeeper_path(root_zookeeper_path_ + "/restore-" + toString(restore_uuid_))
    , all_hosts(all_hosts_)
    , all_hosts_without_initiator(BackupCoordinationOnCluster::excludeInitiator(all_hosts))
    , current_host(current_host_)
    , current_host_index(BackupCoordinationOnCluster::findCurrentHostIndex(current_host, all_hosts))
    , process_list_element(process_list_element_)
    , log(getLogger("RestoreCoordinationOnCluster"))
    , with_retries(log, get_zookeeper_, keeper_settings, process_list_element_, [root_zookeeper_path_](Coordination::ZooKeeperWithFaultInjection::Ptr zk) { zk->sync(root_zookeeper_path_); })
    , cleaner(/* is_restore = */ true, zookeeper_path, with_retries, log)
    , stage_sync(/* is_restore = */ true, fs::path{zookeeper_path} / "stage", current_host, all_hosts, allow_concurrent_restore_, concurrency_counters_, with_retries, schedule_, process_list_element_, log)
{
    try
    {
        createRootNodes();
    }
    catch (...)
    {
        stage_sync.setError(std::current_exception(), /* throw_if_error = */ false);
        throw;
    }
}

RestoreCoordinationOnCluster::~RestoreCoordinationOnCluster() = default;

void RestoreCoordinationOnCluster::createRootNodes()
{
    auto holder = with_retries.createRetriesControlHolder("createRootNodes", WithRetries::kInitialization);
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            zk->createAncestors(zookeeper_path);
            zk->createIfNotExists(zookeeper_path, "");
            zk->createIfNotExists(zookeeper_path + "/repl_databases_tables_acquired", "");
            zk->createIfNotExists(zookeeper_path + "/repl_tables_data_acquired", "");
            zk->createIfNotExists(zookeeper_path + "/repl_access_storages_acquired", "");
            zk->createIfNotExists(zookeeper_path + "/repl_sql_objects_acquired", "");
            zk->createIfNotExists(zookeeper_path + "/keeper_map_tables", "");
            zk->createIfNotExists(zookeeper_path + "/table_uuids", "");
        });
}

void RestoreCoordinationOnCluster::setRestoreQueryIsSentToOtherHosts()
{
    stage_sync.setQueryIsSentToOtherHosts();
}

bool RestoreCoordinationOnCluster::isRestoreQuerySentToOtherHosts() const
{
    return stage_sync.isQuerySentToOtherHosts();
}

Strings RestoreCoordinationOnCluster::setStage(const String & new_stage, const String & message, bool sync)
{
    stage_sync.setStage(new_stage, message);
    if (sync)
        return stage_sync.waitHostsReachStage(all_hosts_without_initiator, new_stage);
    return {};
}

bool RestoreCoordinationOnCluster::setError(std::exception_ptr exception, bool throw_if_error)
{
    return stage_sync.setError(exception, throw_if_error);
}

bool RestoreCoordinationOnCluster::waitOtherHostsFinish(bool throw_if_error) const
{
    return stage_sync.waitOtherHostsFinish(throw_if_error);
}

bool RestoreCoordinationOnCluster::finish(bool throw_if_error)
{
    return stage_sync.finish(throw_if_error);
}

bool RestoreCoordinationOnCluster::cleanup(bool throw_if_error)
{
    /// All the hosts must finish before we remove the coordination nodes.
    bool expect_other_hosts_finished = stage_sync.isQuerySentToOtherHosts() || !stage_sync.isErrorSet();
    bool all_hosts_finished = stage_sync.finished() && (stage_sync.otherHostsFinished() || !expect_other_hosts_finished);
    if (!all_hosts_finished)
    {
        auto unfinished_hosts = expect_other_hosts_finished ? stage_sync.getUnfinishedHosts() : Strings{current_host};
        LOG_INFO(log, "Skipping removing nodes from ZooKeeper because hosts {} didn't finish",
                 BackupCoordinationStageSync::getHostsDesc(unfinished_hosts));
        return false;
    }
    return cleaner.cleanup(throw_if_error);
}

ZooKeeperRetriesInfo RestoreCoordinationOnCluster::getOnClusterInitializationKeeperRetriesInfo() const
{
    return ZooKeeperRetriesInfo{keeper_settings.max_retries_while_initializing,
                                static_cast<UInt64>(keeper_settings.retry_initial_backoff_ms.count()),
                                static_cast<UInt64>(keeper_settings.retry_max_backoff_ms.count()),
                                process_list_element};
}

bool RestoreCoordinationOnCluster::acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name)
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

bool RestoreCoordinationOnCluster::acquireInsertingDataIntoReplicatedTable(const String & table_zk_path)
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

bool RestoreCoordinationOnCluster::acquireReplicatedAccessStorage(const String & access_storage_zk_path)
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

bool RestoreCoordinationOnCluster::acquireReplicatedSQLObjects(const String & loader_zk_path, UserDefinedSQLObjectType object_type)
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
            result = zk->get(path) == toString(current_host_index);
        });
    return result;
}

bool RestoreCoordinationOnCluster::acquireInsertingDataForKeeperMap(const String & root_zk_path, const String & table_unique_id)
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

void RestoreCoordinationOnCluster::generateUUIDForTable(ASTCreateQuery & create_query)
{
    String query_str = create_query.formatWithSecretsOneLine();
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

}
