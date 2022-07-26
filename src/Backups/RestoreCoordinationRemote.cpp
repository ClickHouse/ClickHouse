#include <Backups/RestoreCoordinationRemote.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>


namespace DB
{

RestoreCoordinationRemote::RestoreCoordinationRemote(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_, bool remove_zk_nodes_in_destructor_)
    : zookeeper_path(zookeeper_path_)
    , get_zookeeper(get_zookeeper_)
    , remove_zk_nodes_in_destructor(remove_zk_nodes_in_destructor_)
    , stage_sync(zookeeper_path_ + "/stage", get_zookeeper_, &Poco::Logger::get("RestoreCoordination"))
{
    createRootNodes();
}

RestoreCoordinationRemote::~RestoreCoordinationRemote()
{
    try
    {
        if (remove_zk_nodes_in_destructor)
            removeAllNodes();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void RestoreCoordinationRemote::createRootNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_databases_tables_acquired", "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_tables_data_acquired", "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_access_storages_acquired", "");
}


void RestoreCoordinationRemote::setStage(const String & current_host, const String & new_stage, const String & message)
{
    stage_sync.set(current_host, new_stage, message);
}

void RestoreCoordinationRemote::setError(const String & current_host, const Exception & exception)
{
    stage_sync.setError(current_host, exception);
}

Strings RestoreCoordinationRemote::waitForStage(const Strings & all_hosts, const String & stage_to_wait)
{
    return stage_sync.wait(all_hosts, stage_to_wait);
}

Strings RestoreCoordinationRemote::waitForStage(const Strings & all_hosts, const String & stage_to_wait, std::chrono::milliseconds timeout)
{
    return stage_sync.waitFor(all_hosts, stage_to_wait, timeout);
}


bool RestoreCoordinationRemote::acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name)
{
    auto zookeeper = get_zookeeper();

    String path = zookeeper_path + "/repl_databases_tables_acquired/" + escapeForFileName(database_zk_path);
    zookeeper->createIfNotExists(path, "");

    path += "/" + escapeForFileName(table_name);
    auto code = zookeeper->tryCreate(path, "", zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, path);

    return (code == Coordination::Error::ZOK);
}

bool RestoreCoordinationRemote::acquireInsertingDataIntoReplicatedTable(const String & table_zk_path)
{
    auto zookeeper = get_zookeeper();

    String path = zookeeper_path + "/repl_tables_data_acquired/" + escapeForFileName(table_zk_path);
    auto code = zookeeper->tryCreate(path, "", zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, path);

    return (code == Coordination::Error::ZOK);
}

bool RestoreCoordinationRemote::acquireReplicatedAccessStorage(const String & access_storage_zk_path)
{
    auto zookeeper = get_zookeeper();

    String path = zookeeper_path + "/repl_access_storages_acquired/" + escapeForFileName(access_storage_zk_path);
    auto code = zookeeper->tryCreate(path, "", zkutil::CreateMode::Persistent);
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

    auto zookeeper = get_zookeeper();
    zookeeper->removeRecursive(zookeeper_path);
}

}
