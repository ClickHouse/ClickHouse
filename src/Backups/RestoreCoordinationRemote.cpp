#include <Backups/RestoreCoordinationRemote.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>
#include <Backups/BackupCoordinationStage.h>
#include <Backups/BackupCoordinationRemote.h>

namespace DB
{

namespace Stage = BackupCoordinationStage;

RestoreCoordinationRemote::RestoreCoordinationRemote(
    zkutil::GetZooKeeper get_zookeeper_,
    const String & root_zookeeper_path_,
    const String & restore_uuid_,
    const Strings & all_hosts_,
    const String & current_host_,
    bool is_internal_)
    : get_zookeeper(get_zookeeper_)
    , root_zookeeper_path(root_zookeeper_path_)
    , restore_uuid(restore_uuid_)
    , zookeeper_path(root_zookeeper_path_ + "/restore-" + restore_uuid_)
    , all_hosts(all_hosts_)
    , current_host(current_host_)
    , current_host_index(BackupCoordinationRemote::findCurrentHostIndex(all_hosts, current_host))
    , is_internal(is_internal_)
{
    createRootNodes();

    stage_sync.emplace(
        zookeeper_path + "/stage", [this] { return getZooKeeper(); }, &Poco::Logger::get("RestoreCoordination"));
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

zkutil::ZooKeeperPtr RestoreCoordinationRemote::getZooKeeper() const
{
    std::lock_guard lock{mutex};
    if (!zookeeper || zookeeper->expired())
    {
        zookeeper = get_zookeeper();

        /// It's possible that we connected to different [Zoo]Keeper instance
        /// so we may read a bit stale state.
        zookeeper->sync(zookeeper_path);
    }
    return zookeeper;
}

void RestoreCoordinationRemote::createRootNodes()
{
    auto zk = getZooKeeper();
    zk->createAncestors(zookeeper_path);
    zk->createIfNotExists(zookeeper_path, "");
    zk->createIfNotExists(zookeeper_path + "/repl_databases_tables_acquired", "");
    zk->createIfNotExists(zookeeper_path + "/repl_tables_data_acquired", "");
    zk->createIfNotExists(zookeeper_path + "/repl_access_storages_acquired", "");
    zk->createIfNotExists(zookeeper_path + "/repl_sql_objects_acquired", "");
}


void RestoreCoordinationRemote::setStage(const String & new_stage, const String & message)
{
    stage_sync->set(current_host, new_stage, message);
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
    auto zk = getZooKeeper();

    String path = zookeeper_path + "/repl_databases_tables_acquired/" + escapeForFileName(database_zk_path);
    zk->createIfNotExists(path, "");

    path += "/" + escapeForFileName(table_name);
    auto code = zk->tryCreate(path, "", zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, path);

    return (code == Coordination::Error::ZOK);
}

bool RestoreCoordinationRemote::acquireInsertingDataIntoReplicatedTable(const String & table_zk_path)
{
    auto zk = getZooKeeper();

    String path = zookeeper_path + "/repl_tables_data_acquired/" + escapeForFileName(table_zk_path);
    auto code = zk->tryCreate(path, "", zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, path);

    return (code == Coordination::Error::ZOK);
}

bool RestoreCoordinationRemote::acquireReplicatedAccessStorage(const String & access_storage_zk_path)
{
    auto zk = getZooKeeper();

    String path = zookeeper_path + "/repl_access_storages_acquired/" + escapeForFileName(access_storage_zk_path);
    auto code = zk->tryCreate(path, "", zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, path);

    return (code == Coordination::Error::ZOK);
}

bool RestoreCoordinationRemote::acquireReplicatedSQLObjects(const String & loader_zk_path, UserDefinedSQLObjectType object_type)
{
    auto zk = getZooKeeper();

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
        throw zkutil::KeeperException(code, path);

    return (code == Coordination::Error::ZOK);
}

void RestoreCoordinationRemote::removeAllNodes()
{
    /// Usually this function is called by the initiator when a restore operation is complete so we don't need the coordination anymore.
    ///
    /// However there can be a rare situation when this function is called after an error occurs on the initiator of a query
    /// while some hosts are still restoring something. Removing all the nodes will remove the parent node of the restore coordination
    /// at `zookeeper_path` which might cause such hosts to stop with exception "ZNONODE". Or such hosts might still do some part
    /// of their restore work before that.

    auto zk = getZooKeeper();
    zk->removeRecursive(zookeeper_path);
}

bool RestoreCoordinationRemote::hasConcurrentRestores(const std::atomic<size_t> &) const
{
    /// If its internal concurrency will be checked for the base restore
    if (is_internal)
        return false;

    auto zk = getZooKeeper();
    std::string path = zookeeper_path +"/stage";

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


            const auto status = zk->get(root_zookeeper_path + "/" + existing_restore_path + "/stage");
            if (status != Stage::COMPLETED)
                return true;
        }

        zk->createIfNotExists(path, "");
        auto code = zk->trySet(path, Stage::SCHEDULED_TO_START, stat.version);
        if (code == Coordination::Error::ZOK)
            break;
        bool is_last_attempt = (attempt == MAX_ZOOKEEPER_ATTEMPTS - 1);
        if ((code != Coordination::Error::ZBADVERSION) || is_last_attempt)
            throw zkutil::KeeperException(code, path);
    }
    return false;
}

}
