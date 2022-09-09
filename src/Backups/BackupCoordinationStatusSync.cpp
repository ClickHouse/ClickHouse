#include <Backups/BackupCoordinationStatusSync.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <base/chrono_io.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FAILED_TO_SYNC_BACKUP_OR_RESTORE;
}


BackupCoordinationStatusSync::BackupCoordinationStatusSync(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_, Poco::Logger * log_)
    : zookeeper_path(zookeeper_path_)
    , get_zookeeper(get_zookeeper_)
    , log(log_)
{
    createRootNodes();
}

void BackupCoordinationStatusSync::createRootNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
}

void BackupCoordinationStatusSync::set(const String & current_host, const String & new_status, const String & message)
{
    auto zookeeper = get_zookeeper();
    zookeeper->createIfNotExists(zookeeper_path + "/" + current_host + "|" + new_status, message);
}

void BackupCoordinationStatusSync::setError(const String & current_host, const Exception & exception)
{
    auto zookeeper = get_zookeeper();

    Exception exception2 = exception;
    exception2.addMessage("Host {}", current_host);
    WriteBufferFromOwnString buf;
    writeException(exception2, buf, true);

    zookeeper->createIfNotExists(zookeeper_path + "/error", buf.str());
}

Strings BackupCoordinationStatusSync::wait(const Strings & all_hosts, const String & status_to_wait)
{
    return waitImpl(all_hosts, status_to_wait, {});
}

Strings BackupCoordinationStatusSync::waitFor(const Strings & all_hosts, const String & status_to_wait, UInt64 timeout_ms)
{
    return waitImpl(all_hosts, status_to_wait, timeout_ms);
}

Strings BackupCoordinationStatusSync::waitImpl(const Strings & all_hosts, const String & status_to_wait, std::optional<UInt64> timeout_ms)
{
    if (all_hosts.empty())
        return {};

    /// Wait for other hosts.

    Strings ready_hosts_results;
    ready_hosts_results.resize(all_hosts.size());

    std::map<String, std::vector<size_t> /* index in `ready_hosts_results` */> unready_hosts;
    for (size_t i = 0; i != all_hosts.size(); ++i)
        unready_hosts[all_hosts[i]].push_back(i);

    std::optional<Exception> error;

    auto zookeeper = get_zookeeper();

    /// Process ZooKeeper's nodes and set `all_hosts_ready` or `unready_host` or `error_message`.
    auto process_zk_nodes = [&](const Strings & zk_nodes)
    {
        for (const String & zk_node : zk_nodes)
        {
            if (zk_node.starts_with("remove_watch-"))
                continue;

            if (zk_node == "error")
            {
                ReadBufferFromOwnString buf{zookeeper->get(zookeeper_path + "/error")};
                error = readException(buf, "", true);
                break;
            }

            size_t separator_pos = zk_node.find('|');
            if (separator_pos == String::npos)
                throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Unexpected zk node {}", zookeeper_path + "/" + zk_node);

            String host = zk_node.substr(0, separator_pos);
            String status = zk_node.substr(separator_pos + 1);

            auto it = unready_hosts.find(host);
            if ((it != unready_hosts.end()) && (status == status_to_wait))
            {
                String result = zookeeper->get(zookeeper_path + "/" + zk_node);
                for (size_t i : it->second)
                    ready_hosts_results[i] = result;
                unready_hosts.erase(it);
            }
        }
    };

    /// Wait until all hosts are ready or an error happens or time is out.
    std::atomic<bool> watch_set = false;
    std::condition_variable watch_triggered_event;

    auto watch_callback = [&](const Coordination::WatchResponse &)
    {
        watch_set = false; /// After it's triggered it's not set until we call getChildrenWatch() again.
        watch_triggered_event.notify_all();
    };

    auto watch_triggered = [&] { return !watch_set; };

    bool use_timeout = timeout_ms.has_value();
    std::chrono::milliseconds timeout{timeout_ms.value_or(0)};
    std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
    std::chrono::steady_clock::duration elapsed;
    std::mutex dummy_mutex;
    String previous_unready_host;

    while (!unready_hosts.empty() && !error)
    {
        watch_set = true;
        Strings nodes = zookeeper->getChildrenWatch(zookeeper_path, nullptr, watch_callback);
        process_zk_nodes(nodes);

        if (!unready_hosts.empty() && !error)
        {
            const auto & unready_host = unready_hosts.begin()->first;
            if (unready_host != previous_unready_host)
            {
                LOG_TRACE(log, "Waiting for host {}", unready_host);
                previous_unready_host = unready_host;
            }

            std::unique_lock dummy_lock{dummy_mutex};
            if (use_timeout)
            {
                elapsed = std::chrono::steady_clock::now() - start_time;
                if ((elapsed > timeout) || !watch_triggered_event.wait_for(dummy_lock, timeout - elapsed, watch_triggered))
                    break;
            }
            else
                watch_triggered_event.wait(dummy_lock, watch_triggered);
        }
    }

    if (watch_set)
    {
        /// Remove watch by triggering it.
        zookeeper->create(zookeeper_path + "/remove_watch-", "", zkutil::CreateMode::EphemeralSequential);
        std::unique_lock dummy_lock{dummy_mutex};
        watch_triggered_event.wait(dummy_lock, watch_triggered);
    }

    if (error)
        error->rethrow();

    if (!unready_hosts.empty())
    {
        throw Exception(
            ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
            "Waited for host {} too long ({})",
            unready_hosts.begin()->first,
            to_string(elapsed));
    }

    return ready_hosts_results;
}

}
