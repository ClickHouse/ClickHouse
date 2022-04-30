#include <Backups/RestoreCoordinationDistributed.h>
#include <Backups/formatTableNameOrTemporaryTableName.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <base/logger_useful.h>
#include <base/chrono_io.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int FAILED_TO_RESTORE_METADATA_ON_OTHER_NODE;
}

namespace
{
    struct TableExistedStatus
    {
        DatabaseAndTableName table_name;
        bool table_existed = false;

        void write(WriteBuffer & out) const
        {
            writeBinary(table_name.first, out);
            writeBinary(table_name.second, out);
            writeBinary(table_existed, out);
        }

        void read(ReadBuffer & in)
        {
            readBinary(table_name.first, in);
            readBinary(table_name.second, in);
            readBinary(table_existed, in);
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
    zookeeper->createIfNotExists(zookeeper_path + "/hosts_metadata", "");
    zookeeper->createIfNotExists(zookeeper_path + "/tables_existed", "");
    zookeeper->createIfNotExists(zookeeper_path + "/data_paths", "");
    zookeeper->createIfNotExists(zookeeper_path + "/partitions", "");
}

void RestoreCoordinationDistributed::removeAllNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->removeRecursive(zookeeper_path);
}

void RestoreCoordinationDistributed::finishRestoringMetadata(const String & host_id_, const String & error_message_)
{
    LOG_TRACE(log, "Finished restoring metadata{}", (error_message_.empty() ? "" : (" with error " + error_message_)));
    auto zookeeper = get_zookeeper();
    if (error_message_.empty())
        zookeeper->create(zookeeper_path + "/hosts_metadata/" + host_id_ + ":ready", "", zkutil::CreateMode::Persistent);
    else
        zookeeper->create(zookeeper_path + "/hosts_metadata/" + host_id_ + ":error", error_message_, zkutil::CreateMode::Persistent);
}

void RestoreCoordinationDistributed::waitHostsToRestoreMetadata(const Strings & host_ids_, std::chrono::seconds timeout_) const
{
    auto zookeeper = get_zookeeper();

    std::mutex mutex;
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
                std::lock_guard lock{mutex};
                error_host_id = host_id;
                error_message = zookeeper->get(zookeeper_path + "/hosts_metadata/" + host_id + ":error");
                return;
            }
            if (!set.contains(host_id + ":ready"))
            {
                std::lock_guard lock{mutex};
                LOG_TRACE(log, "Waiting for host {} to restore its metadata", host_id);
                not_ready_host_id = host_id;
                return;
            }
        }

        {
            std::lock_guard lock{mutex};
            all_hosts_ready = true;
        }
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
        Strings children = zookeeper->getChildrenWatch(zookeeper_path + "/hosts_metadata", nullptr, watch_callback);
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
        zookeeper->create(zookeeper_path + "/hosts_metadata/remove_watch-", "", zkutil::CreateMode::EphemeralSequential);
        std::unique_lock dummy_lock{dummy_mutex};
        watch_triggered_event.wait_for(dummy_lock, timeout_, watch_triggered);
    }

    throw Exception(
        ErrorCodes::FAILED_TO_RESTORE_METADATA_ON_OTHER_NODE,
        "Host {} was unable to restore its metadata in {}",
        not_ready_host_id,
        to_string(timeout_));
}

void RestoreCoordinationDistributed::setTableExistedInReplicatedDB(const String & /* host_id_ */,
                                                                   const String & database_name_,
                                                                   const String & database_zk_path_,
                                                                   const String & table_name_,
                                                                   bool table_existed_)
{
    auto zookeeper = get_zookeeper();

    String path = zookeeper_path + "/tables_existed/" + escapeForFileName(database_zk_path_);
    zookeeper->createIfNotExists(path, "");

    path += "/" + escapeForFileName(table_name_);

    String status_str;
    {
        TableExistedStatus status;
        status.table_name.first = database_name_;
        status.table_name.second = table_name_;
        status.table_existed = table_existed_;
        WriteBufferFromOwnString buf;
        status.write(buf);
        status_str = buf.str();
    }

    auto code = zookeeper->tryCreate(path, status_str, zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, path);

    if (code == Coordination::Error::ZOK)
        return;

    if (!table_existed_)
        zookeeper->set(path, status_str);
}

void RestoreCoordinationDistributed::checkTablesNotExistedInReplicatedDBs() const
{
    auto zookeeper = get_zookeeper();

    for (const String & database_zk_path_escaped : zookeeper->getChildren(zookeeper_path + "/tables_existed"))
    {
        for (const String & table_name_escaped : zookeeper->getChildren(zookeeper_path + "/tables_existed/" + database_zk_path_escaped))
        {
            String status_str = zookeeper->get(zookeeper_path + "/tables_existed/" + database_zk_path_escaped + "/" + table_name_escaped);
            TableExistedStatus status;
            ReadBufferFromString buf{status_str};
            status.read(buf);
            if (status.table_existed)
                throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "{} already exists", formatTableNameOrTemporaryTableName(status.table_name));
        }
    }
}

void RestoreCoordinationDistributed::setReplicatedTableDataPath(
    const String & host_id_,
    const DatabaseAndTableName & table_name_,
    const String & table_zk_path_,
    const String & data_path_in_backup_)
{
    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/data_paths/" + escapeForFileName(table_zk_path_);

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
    String path = zookeeper_path + "/data_paths/" + escapeForFileName(table_zk_path_);
    String info_str = zookeeper->get(path);
    ReadBufferFromString buf{info_str};
    ReplicatedTableDataPath info;
    info.read(buf);
    return info.data_path_in_backup;
}

bool RestoreCoordinationDistributed::startRestoringReplicatedTablePartition(
    const String & host_id_,
    const DatabaseAndTableName & table_name_,
    const String & table_zk_path_,
    const String & partition_name_)
{
    auto zookeeper = get_zookeeper();

    String path = zookeeper_path + "/partitions/" + escapeForFileName(table_zk_path_);
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
