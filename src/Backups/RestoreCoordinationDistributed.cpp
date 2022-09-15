#include <Backups/RestoreCoordinationDistributed.h>
#include <Backups/formatTableNameOrTemporaryTableName.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
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

namespace
{
    struct ReplicatedTableDataPath
    {
        String host_id;
        DatabaseAndTableName table_name;
        String data_path_in_backup;

        String serialize() const
        {
            WriteBufferFromOwnString out;
            writeBinary(host_id, out);
            writeBinary(table_name.first, out);
            writeBinary(table_name.second, out);
            writeBinary(data_path_in_backup, out);
            return out.str();
        }

        static ReplicatedTableDataPath deserialize(const String & str)
        {
            ReadBufferFromString in{str};
            ReplicatedTableDataPath res;
            readBinary(res.host_id, in);
            readBinary(res.table_name.first, in);
            readBinary(res.table_name.second, in);
            readBinary(res.data_path_in_backup, in);
            return res;
        }
    };
}


class RestoreCoordinationDistributed::ReplicatedDatabasesMetadataSync
{
public:
    ReplicatedDatabasesMetadataSync(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_)
        : zookeeper_path(zookeeper_path_), get_zookeeper(get_zookeeper_), log(&Poco::Logger::get("RestoreCoordination"))
    {
        createRootNodes();
    }

    /// Starts creating a table in a replicated database. Returns false if there is another host which is already creating this table.
    bool startCreatingTable(
        const String & host_id_, const String & database_name_, const String & database_zk_path_, const String & table_name_)
    {
        auto zookeeper = get_zookeeper();

        String path = zookeeper_path + "/" + escapeForFileName(database_zk_path_);
        zookeeper->createIfNotExists(path, "");

        TableStatus status;
        status.host_id = host_id_;
        status.table_name = DatabaseAndTableName{database_name_, table_name_};

        path += "/" + escapeForFileName(table_name_);
        auto code = zookeeper->tryCreate(path, status.serialize(), zkutil::CreateMode::Persistent);
        if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
            throw zkutil::KeeperException(code, path);

        return (code == Coordination::Error::ZOK);
    }

    /// Sets that either we have been created a table in a replicated database or failed doing that.
    /// In the latter case `error_message` should be set.
    /// Calling this function unblocks other hosts waiting for this table to be created (see waitForCreatingTableInReplicatedDB()).
    void finishCreatingTable(
        const String & /* host_id_ */,
        const String & database_name_,
        const String & database_zk_path_,
        const String & table_name_,
        const String & error_message_)
    {
        if (error_message_.empty())
            LOG_TRACE(log, "Created table {}.{}", database_name_, table_name_);
        else
            LOG_TRACE(log, "Failed to created table {}.{}: {}", database_name_, table_name_, error_message_);

        auto zookeeper = get_zookeeper();
        String path = zookeeper_path + "/" + escapeForFileName(database_zk_path_) + "/" + escapeForFileName(table_name_);

        auto status = TableStatus::deserialize(zookeeper->get(path));

        status.error_message = error_message_;
        status.ready = error_message_.empty();

        zookeeper->set(path, status.serialize());
    }

    /// Wait for another host to create a table in a replicated database.
    void waitForTableCreated(
        const String & /* database_name_ */, const String & database_zk_path_, const String & table_name_, std::chrono::seconds timeout_)
    {
        auto zookeeper = get_zookeeper();
        String path = zookeeper_path + "/" + escapeForFileName(database_zk_path_) + "/" + escapeForFileName(table_name_);

        TableStatus status;

        std::atomic<bool> watch_set = false;
        std::condition_variable watch_triggered_event;

        auto watch_callback = [&](const Coordination::WatchResponse &)
        {
            watch_set = false; /// After it's triggered it's not set until we call getChildrenWatch() again.
            watch_triggered_event.notify_all();
        };

        auto watch_triggered = [&] { return !watch_set; };

        bool use_timeout = (timeout_.count() >= 0);
        std::chrono::steady_clock::duration time_left = timeout_;
        std::mutex dummy_mutex;

        while (true)
        {
            if (use_timeout && (time_left.count() <= 0))
            {
                status = TableStatus::deserialize(zookeeper->get(path));
                break;
            }

            watch_set = true;
            status = TableStatus::deserialize(zookeeper->getWatch(path, nullptr, watch_callback));

            if (!status.error_message.empty() || status.ready)
                break;

            LOG_TRACE(log, "Waiting for host {} to create table {}.{}", status.host_id, status.table_name.first, status.table_name.second);

            {
                std::unique_lock dummy_lock{dummy_mutex};
                if (use_timeout)
                {
                    std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
                    if (!watch_triggered_event.wait_for(dummy_lock, time_left, watch_triggered))
                        break;
                    time_left -= (std::chrono::steady_clock::now() - start_time);
                }
                else
                    watch_triggered_event.wait(dummy_lock, watch_triggered);
            }
        }

        if (watch_set)
        {
            /// Remove watch by triggering it.
            ++status.increment;
            zookeeper->set(path, status.serialize());
            std::unique_lock dummy_lock{dummy_mutex};
            watch_triggered_event.wait_for(dummy_lock, timeout_, watch_triggered);
        }

        if (!status.error_message.empty())
            throw Exception(
                ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                "Host {} failed to create table {}.{}: {}", status.host_id, status.table_name.first, status.table_name.second, status.error_message);

        if (status.ready)
        {
            LOG_TRACE(log, "Host {} created table {}.{}", status.host_id, status.table_name.first, status.table_name.second);
            return;
        }

        throw Exception(
            ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
            "Host {} was unable to create table {}.{} in {}",
            status.host_id,
            status.table_name.first,
            table_name_,
            to_string(timeout_));
    }

private:
    void createRootNodes()
    {
        auto zookeeper = get_zookeeper();
        zookeeper->createAncestors(zookeeper_path);
        zookeeper->createIfNotExists(zookeeper_path, "");
    }

    struct TableStatus
    {
        String host_id;
        DatabaseAndTableName table_name;
        bool ready = false;
        String error_message;
        size_t increment = 0;

        String serialize() const
        {
            WriteBufferFromOwnString out;
            writeBinary(host_id, out);
            writeBinary(table_name.first, out);
            writeBinary(table_name.second, out);
            writeBinary(ready, out);
            writeBinary(error_message, out);
            writeBinary(increment, out);
            return out.str();
        }

        static TableStatus deserialize(const String & str)
        {
            ReadBufferFromString in{str};
            TableStatus res;
            readBinary(res.host_id, in);
            readBinary(res.table_name.first, in);
            readBinary(res.table_name.second, in);
            readBinary(res.ready, in);
            readBinary(res.error_message, in);
            readBinary(res.increment, in);
            return res;
        }
    };

    const String zookeeper_path;
    const zkutil::GetZooKeeper get_zookeeper;
    const Poco::Logger * log;
};


RestoreCoordinationDistributed::RestoreCoordinationDistributed(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_)
    : zookeeper_path(zookeeper_path_)
    , get_zookeeper(get_zookeeper_)
    , replicated_databases_metadata_sync(
          std::make_unique<ReplicatedDatabasesMetadataSync>(zookeeper_path_ + "/repl_databases_metadata", get_zookeeper_))
    , all_metadata_barrier(zookeeper_path_ + "/all_metadata", get_zookeeper_, "RestoreCoordination", "restoring metadata")
{
    createRootNodes();
}

RestoreCoordinationDistributed::~RestoreCoordinationDistributed() = default;

void RestoreCoordinationDistributed::createRootNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_tables_paths", "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_tables_partitions", "");
}

void RestoreCoordinationDistributed::removeAllNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->removeRecursive(zookeeper_path);
}

bool RestoreCoordinationDistributed::startCreatingTableInReplicatedDB(
    const String & host_id, const String & database_name, const String & database_zk_path, const String & table_name)
{
    return replicated_databases_metadata_sync->startCreatingTable(host_id, database_name, database_zk_path, table_name);
}

/// Ends creating table in a replicated database, successfully or with an error.
/// In the latter case `error_message` should be set.
void RestoreCoordinationDistributed::finishCreatingTableInReplicatedDB(
    const String & host_id,
    const String & database_name,
    const String & database_zk_path,
    const String & table_name,
    const String & error_message)
{
    return replicated_databases_metadata_sync->finishCreatingTable(host_id, database_name, database_zk_path, table_name, error_message);
}

/// Wait for another host to create a table in a replicated database.
void RestoreCoordinationDistributed::waitForTableCreatedInReplicatedDB(
    const String & database_name, const String & database_zk_path, const String & table_name, std::chrono::seconds timeout)
{
    return replicated_databases_metadata_sync->waitForTableCreated(database_name, database_zk_path, table_name, timeout);
}

void RestoreCoordinationDistributed::finishRestoringMetadata(const String & host_id, const String & error_message)
{
    all_metadata_barrier.finish(host_id, error_message);
}

void RestoreCoordinationDistributed::waitForAllHostsRestoredMetadata(const Strings & host_ids, std::chrono::seconds timeout) const
{
    all_metadata_barrier.waitForAllHostsToFinish(host_ids, timeout);
}

void RestoreCoordinationDistributed::addReplicatedTableDataPath(
    const String & host_id,
    const DatabaseAndTableName & table_name,
    const String & table_zk_path,
    const String & data_path_in_backup)
{
    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/repl_tables_paths/" + escapeForFileName(table_zk_path);

    ReplicatedTableDataPath new_info;
    new_info.host_id = host_id;
    new_info.table_name = table_name;
    new_info.data_path_in_backup = data_path_in_backup;
    String new_info_str = new_info.serialize();

    auto code = zookeeper->tryCreate(path, new_info_str, zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, path);

    while (code != Coordination::Error::ZOK)
    {
        Coordination::Stat stat;
        ReplicatedTableDataPath cur_info = ReplicatedTableDataPath::deserialize(zookeeper->get(path, &stat));
        if ((cur_info.host_id < host_id) || ((cur_info.host_id == host_id) && (cur_info.table_name <= table_name)))
            break;
        code = zookeeper->trySet(path, new_info_str, stat.version);
        if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZBADVERSION))
            throw zkutil::KeeperException(code, path);
    }
}

String RestoreCoordinationDistributed::getReplicatedTableDataPath(const String & table_zk_path_) const
{
    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/repl_tables_paths/" + escapeForFileName(table_zk_path_);
    auto info = ReplicatedTableDataPath::deserialize(zookeeper->get(path));
    return info.data_path_in_backup;
}

bool RestoreCoordinationDistributed::startInsertingDataToPartitionInReplicatedTable(
    const String & host_id_,
    const DatabaseAndTableName & table_name_,
    const String & table_zk_path_,
    const String & partition_name_)
{
    auto zookeeper = get_zookeeper();

    String path = zookeeper_path + "/repl_tables_partitions/" + escapeForFileName(table_zk_path_);
    zookeeper->createIfNotExists(path, "");

    path += "/" + escapeForFileName(partition_name_);
    String new_info = host_id_ + "|" + table_name_.first + "|" + table_name_.second;

    auto code = zookeeper->tryCreate(path, new_info, zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, path);

    if (code == Coordination::Error::ZOK)
        return true;

    return zookeeper->get(path) == new_info;
}

void RestoreCoordinationDistributed::drop()
{
    removeAllNodes();
}

}
