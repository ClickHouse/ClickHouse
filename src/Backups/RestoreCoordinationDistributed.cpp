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
    extern const int FAILED_TO_RESTORE_METADATA_ON_OTHER_NODE;
}

namespace
{
    struct TableInReplicatedDatabaseStatus
    {
        String host_id;
        DatabaseAndTableName table_name;
        bool ready = false;
        String error_message;
        size_t increment = 0;

        void write(WriteBuffer & out) const
        {
            writeBinary(host_id, out);
            writeBinary(table_name.first, out);
            writeBinary(table_name.second, out);
            writeBinary(ready, out);
            writeBinary(error_message, out);
            writeBinary(increment, out);
        }

        void read(ReadBuffer & in)
        {
            readBinary(host_id, in);
            readBinary(table_name.first, in);
            readBinary(table_name.second, in);
            readBinary(ready, in);
            readBinary(error_message, in);
            readBinary(increment, in);
        }
    };

    struct ReplicatedTableDataPath
    {
        String host_id;
        DatabaseAndTableName table_name;
        String data_path_in_backup;

        void write(WriteBuffer & out) const
        {
            writeBinary(host_id, out);
            writeBinary(table_name.first, out);
            writeBinary(table_name.second, out);
            writeBinary(data_path_in_backup, out);
        }

        void read(ReadBuffer & in)
        {
            readBinary(host_id, in);
            readBinary(table_name.first, in);
            readBinary(table_name.second, in);
            readBinary(data_path_in_backup, in);
        }
    };
}


RestoreCoordinationDistributed::RestoreCoordinationDistributed(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_)
    : zookeeper_path(zookeeper_path_), get_zookeeper(get_zookeeper_), log(&Poco::Logger::get("RestoreCoordinationDistributed"))
{
    createRootNodes();
}

RestoreCoordinationDistributed::~RestoreCoordinationDistributed() = default;

void RestoreCoordinationDistributed::createRootNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
    zookeeper->createIfNotExists(zookeeper_path + "/tables_in_repl_databases", "");
    zookeeper->createIfNotExists(zookeeper_path + "/metadata_ready", "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_tables_data_paths", "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_tables_partitions", "");
}

void RestoreCoordinationDistributed::removeAllNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->removeRecursive(zookeeper_path);
}

bool RestoreCoordinationDistributed::startCreatingTableInReplicatedDB(
    const String & host_id_, const String & database_name_, const String & database_zk_path_, const String & table_name_)
{
    auto zookeeper = get_zookeeper();

    String path = zookeeper_path + "/tables_in_repl_databases/" + escapeForFileName(database_zk_path_);
    zookeeper->createIfNotExists(path, "");

    TableInReplicatedDatabaseStatus status;
    status.host_id = host_id_;
    status.table_name = DatabaseAndTableName{database_name_, table_name_};
    String status_str;
    {
        WriteBufferFromOwnString buf;
        status.write(buf);
        status_str = buf.str();
    }

    path += "/" + escapeForFileName(table_name_);

    auto code = zookeeper->tryCreate(path, status_str, zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, path);

    return (code == Coordination::Error::ZOK);
}

/// Ends creating table in a replicated database, successfully or with an error.
/// In the latter case `error_message` should be set.
void RestoreCoordinationDistributed::finishCreatingTableInReplicatedDB(
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
    String path = zookeeper_path + "/tables_in_repl_databases/" + escapeForFileName(database_zk_path_) + "/" + escapeForFileName(table_name_);

    TableInReplicatedDatabaseStatus status;
    String status_str = zookeeper->get(path);
    {
        ReadBufferFromString buf{status_str};
        status.read(buf);
    }

    status.error_message = error_message_;
    status.ready = error_message_.empty();

    {
        WriteBufferFromOwnString buf;
        status.write(buf);
        status_str = buf.str();
    }

    zookeeper->set(path, status_str);
}

/// Wait for another host to create a table in a replicated database.
void RestoreCoordinationDistributed::waitForCreatingTableInReplicatedDB(
    const String & /* database_name_ */, const String & database_zk_path_, const String & table_name_, std::chrono::seconds timeout_)
{
    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/tables_in_repl_databases/" + escapeForFileName(database_zk_path_) + "/" + escapeForFileName(table_name_);

    TableInReplicatedDatabaseStatus status;

    std::atomic<bool> watch_set = false;
    std::condition_variable watch_triggered_event;

    auto watch_callback = [&](const Coordination::WatchResponse &)
    {
        watch_set = false; /// After it's triggered it's not set until we call getChildrenWatch() again.
        watch_triggered_event.notify_all();
    };

    auto watch_triggered = [&] { return !watch_set; };

    bool use_timeout = (timeout_.count() > 0);
    std::chrono::steady_clock::duration time_left = timeout_;
    std::mutex dummy_mutex;

    while (!use_timeout || (time_left.count() > 0))
    {
        watch_set = true;
        String status_str = zookeeper->getWatch(path, nullptr, watch_callback);
        {
            ReadBufferFromString buf{status_str};
            status.read(buf);
        }

        if (!status.error_message.empty())
            throw Exception(
                ErrorCodes::FAILED_TO_RESTORE_METADATA_ON_OTHER_NODE,
                "Host {} failed to create table {}.{}: {}", status.host_id, status.table_name.first, status.table_name.second, status.error_message);

        if (status.ready)
        {
            LOG_TRACE(log, "Host {} created table {}.{}", status.host_id, status.table_name.first, status.table_name.second);
            return;
        }

        LOG_TRACE(log, "Waiting for host {} to create table {}.{}", status.host_id, status.table_name.first, status.table_name.second);

        std::chrono::steady_clock::time_point start_time;
        if (use_timeout)
            start_time = std::chrono::steady_clock::now();

        bool waited;
        {
            std::unique_lock dummy_lock{dummy_mutex};
            if (use_timeout)
            {
                waited = watch_triggered_event.wait_for(dummy_lock, time_left, watch_triggered);
            }
            else
            {
                watch_triggered_event.wait(dummy_lock, watch_triggered);
                waited = true;
            }
        }

        if (use_timeout)
        {
            time_left -= (std::chrono::steady_clock::now() - start_time);
            if (time_left.count() < 0)
                time_left = std::chrono::steady_clock::duration::zero();
        }

        if (!waited)
            break;
    }

    if (watch_set)
    {
        /// Remove watch by triggering it.
        ++status.increment;
        WriteBufferFromOwnString buf;
        status.write(buf);
        zookeeper->set(path, buf.str());
        std::unique_lock dummy_lock{dummy_mutex};
        watch_triggered_event.wait_for(dummy_lock, timeout_, watch_triggered);
    }

    throw Exception(
        ErrorCodes::FAILED_TO_RESTORE_METADATA_ON_OTHER_NODE,
        "Host {} was unable to create table {}.{} in {}",
        status.host_id,
        status.table_name.first,
        table_name_,
        to_string(timeout_));
}

void RestoreCoordinationDistributed::finishRestoringMetadata(const String & host_id_, const String & error_message_)
{
    LOG_TRACE(log, "Finished restoring metadata{}", (error_message_.empty() ? "" : (" with error " + error_message_)));
    auto zookeeper = get_zookeeper();
    if (error_message_.empty())
        zookeeper->create(zookeeper_path + "/metadata_ready/" + host_id_ + ":ready", "", zkutil::CreateMode::Persistent);
    else
        zookeeper->create(zookeeper_path + "/metadata_ready/" + host_id_ + ":error", error_message_, zkutil::CreateMode::Persistent);
}

void RestoreCoordinationDistributed::waitForAllHostsToRestoreMetadata(const Strings & host_ids_, std::chrono::seconds timeout_) const
{
    auto zookeeper = get_zookeeper();

    bool all_hosts_ready = false;
    String not_ready_host_id;
    String error_host_id;
    String error_message;

    /// Returns true of everything's ready, or false if we need to wait more.
    auto process_nodes = [&](const Strings & nodes)
    {
        std::unordered_set<std::string_view> set{nodes.begin(), nodes.end()};
        for (const String & host_id : host_ids_)
        {
            if (set.contains(host_id + ":error"))
            {
                error_host_id = host_id;
                error_message = zookeeper->get(zookeeper_path + "/metadata_ready/" + host_id + ":error");
                return;
            }
            if (!set.contains(host_id + ":ready"))
            {
                LOG_TRACE(log, "Waiting for host {} to restore its metadata", host_id);
                not_ready_host_id = host_id;
                return;
            }
        }

        all_hosts_ready = true;
    };

    std::atomic<bool> watch_set = false;
    std::condition_variable watch_triggered_event;

    auto watch_callback = [&](const Coordination::WatchResponse &)
    {
        watch_set = false; /// After it's triggered it's not set until we call getChildrenWatch() again.
        watch_triggered_event.notify_all();
    };

    auto watch_triggered = [&] { return !watch_set; };

    bool use_timeout = (timeout_.count() > 0);
    std::chrono::steady_clock::duration time_left = timeout_;
    std::mutex dummy_mutex;

    while (!use_timeout || (time_left.count() > 0))
    {
        watch_set = true;
        Strings children = zookeeper->getChildrenWatch(zookeeper_path + "/metadata_ready", nullptr, watch_callback);
        process_nodes(children);

        if (!error_message.empty())
            throw Exception(
                ErrorCodes::FAILED_TO_RESTORE_METADATA_ON_OTHER_NODE,
                "Host {} was unable to restore its metadata: {}",
                error_host_id,
                error_message);

        if (all_hosts_ready)
        {
            LOG_TRACE(log, "All hosts have finished restoring metadata");
            return;
        }

        std::chrono::steady_clock::time_point start_time;
        if (use_timeout)
            start_time = std::chrono::steady_clock::now();

        bool waited;
        {
            std::unique_lock dummy_lock{dummy_mutex};
            if (use_timeout)
            {
                waited = watch_triggered_event.wait_for(dummy_lock, time_left, watch_triggered);
            }
            else
            {
                watch_triggered_event.wait(dummy_lock, watch_triggered);
                waited = true;
            }
        }

        if (use_timeout)
        {
            time_left -= (std::chrono::steady_clock::now() - start_time);
            if (time_left.count() < 0)
                time_left = std::chrono::steady_clock::duration::zero();
        }

        if (!waited)
            break;
    }

    if (watch_set)
    {
        /// Remove watch by triggering it.
        zookeeper->create(zookeeper_path + "/metadata_ready/remove_watch-", "", zkutil::CreateMode::EphemeralSequential);
        std::unique_lock dummy_lock{dummy_mutex};
        watch_triggered_event.wait_for(dummy_lock, timeout_, watch_triggered);
    }

    throw Exception(
        ErrorCodes::FAILED_TO_RESTORE_METADATA_ON_OTHER_NODE,
        "Host {} was unable to restore its metadata in {}",
        not_ready_host_id,
        to_string(timeout_));
}

void RestoreCoordinationDistributed::setReplicatedTableDataPath(
    const String & host_id_,
    const DatabaseAndTableName & table_name_,
    const String & table_zk_path_,
    const String & data_path_in_backup_)
{
    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/repl_tables_data_paths/" + escapeForFileName(table_zk_path_);

    String new_info_str;
    {
        ReplicatedTableDataPath new_info;
        new_info.host_id = host_id_;
        new_info.table_name = table_name_;
        new_info.data_path_in_backup = data_path_in_backup_;
        WriteBufferFromOwnString buf;
        new_info.write(buf);
        new_info_str = buf.str();
    }

    auto code = zookeeper->tryCreate(path, new_info_str, zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, path);

    while (code != Coordination::Error::ZOK)
    {
        Coordination::Stat stat;
        String cur_info_str = zookeeper->get(path, &stat);
        ReadBufferFromString buf{cur_info_str};
        ReplicatedTableDataPath cur_info;
        cur_info.read(buf);
        if ((cur_info.host_id < host_id_) || ((cur_info.host_id == host_id_) && (cur_info.table_name <= table_name_)))
            break;
        code = zookeeper->trySet(path, new_info_str, stat.version);
        if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZBADVERSION))
            throw zkutil::KeeperException(code, path);
    }
}

String RestoreCoordinationDistributed::getReplicatedTableDataPath(const String & table_zk_path_) const
{
    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/repl_tables_data_paths/" + escapeForFileName(table_zk_path_);
    String info_str = zookeeper->get(path);
    ReadBufferFromString buf{info_str};
    ReplicatedTableDataPath info;
    info.read(buf);
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
