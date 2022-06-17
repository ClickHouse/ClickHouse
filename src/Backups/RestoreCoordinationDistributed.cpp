#include <Backups/RestoreCoordinationDistributed.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Interpreters/StorageID.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <base/chrono_io.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FAILED_TO_SYNC_BACKUP_OR_RESTORE;
}


RestoreCoordinationDistributed::RestoreCoordinationDistributed(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_)
    : zookeeper_path(zookeeper_path_)
    , get_zookeeper(get_zookeeper_)
    , stage_sync(zookeeper_path_ + "/stage", get_zookeeper_, &Poco::Logger::get("RestoreCoordination"))
{
    createRootNodes();
}

RestoreCoordinationDistributed::~RestoreCoordinationDistributed() = default;

void RestoreCoordinationDistributed::createRootNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_databases_tables_acquired", "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_tables_data_acquired", "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_access_storages_acquired", "");
}

void RestoreCoordinationDistributed::syncStage(const String & current_host, int new_stage, const Strings & wait_hosts, std::chrono::seconds timeout)
{
    stage_sync.syncStage(current_host, new_stage, wait_hosts, timeout);
}

void RestoreCoordinationDistributed::syncStageError(const String & current_host, const String & error_message)
{
    stage_sync.syncStageError(current_host, error_message);
}

bool RestoreCoordinationDistributed::acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name)
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

bool RestoreCoordinationDistributed::acquireInsertingDataIntoReplicatedTable(const String & table_zk_path)
{
    auto zookeeper = get_zookeeper();

    String path = zookeeper_path + "/repl_tables_data_acquired/" + escapeForFileName(table_zk_path);
    auto code = zookeeper->tryCreate(path, "", zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, path);

    return (code == Coordination::Error::ZOK);
}

bool RestoreCoordinationDistributed::acquireReplicatedAccessStorage(const String & access_storage_zk_path)
{
    auto zookeeper = get_zookeeper();

    String path = zookeeper_path + "/repl_access_storages_acquired/" + escapeForFileName(access_storage_zk_path);
    auto code = zookeeper->tryCreate(path, "", zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, path);

    return (code == Coordination::Error::ZOK);
}

void RestoreCoordinationDistributed::removeAllNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->removeRecursive(zookeeper_path);
}

void RestoreCoordinationDistributed::drop()
{
    removeAllNodes();
}

}
