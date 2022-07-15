#include <Backups/RestoreCoordinationRemote.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>


namespace DB
{

RestoreCoordinationRemote::RestoreCoordinationRemote(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_)
    : zookeeper_path(zookeeper_path_)
    , get_zookeeper(get_zookeeper_)
    , status_sync(zookeeper_path_ + "/status", get_zookeeper_, &Poco::Logger::get("RestoreCoordination"))
{
    createRootNodes();
}

RestoreCoordinationRemote::~RestoreCoordinationRemote() = default;

void RestoreCoordinationRemote::createRootNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_databases_tables_acquired", "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_tables_data_acquired", "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_access_storages_acquired", "");
}

void RestoreCoordinationRemote::setStatus(const String & current_host, const String & new_status, const String & message)
{
    status_sync.set(current_host, new_status, message);
}

Strings RestoreCoordinationRemote::setStatusAndWait(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts)
{
    return status_sync.setAndWait(current_host, new_status, message, all_hosts);
}

Strings RestoreCoordinationRemote::setStatusAndWaitFor(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts, UInt64 timeout_ms)
{
    return status_sync.setAndWaitFor(current_host, new_status, message, all_hosts, timeout_ms);
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
    auto zookeeper = get_zookeeper();
    zookeeper->removeRecursive(zookeeper_path);
}

void RestoreCoordinationRemote::drop()
{
    removeAllNodes();
}

}
