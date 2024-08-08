#include <Backups/BackupCoordinationStageSync.h>

#include <base/chrono_io.h>
#include <Common/ZooKeeper/Common.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Backups/BackupCoordinationStage.h>

namespace DB
{

namespace Stage = BackupCoordinationStage;

namespace ErrorCodes
{
    extern const int FAILED_TO_SYNC_BACKUP_OR_RESTORE;
}


BackupCoordinationStageSync::BackupCoordinationStageSync(
    const String & root_zookeeper_path_,
    WithRetries & with_retries_,
    LoggerPtr log_)
    : zookeeper_path(root_zookeeper_path_ + "/stage")
    , with_retries(with_retries_)
    , log(log_)
{
    task = schedule_pool.createTask(log_name, [this]{ run(); });
}

BackupCoordinationStageSync::~BackupCoordinationStageSync()
{
    shutdown();
}

void BackupCoordinationStageSync::run()
{
    try
    {
        runImpl();
    }
    catch (...)
    {
        consecutive_check_failures++;
        if (!shutdown)
            task->scheduleAfter(next_failure_retry_ms);
    }
}


void BackupCoordinationStageSync::runImpl()
{
    while (!shutdown)
    {
        createRootNodes();
        createEphemeralNode(zookeeper);
        readState(zookeeper);
        zk_nodes_changed.tryWait(check_period_ms);
    }
}

void BackupCoordinationStageSync::createRootNodes(FaultyKeeper zookeeper)
{
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
}

void BackupCoordinationStageSync::createEphemeralNode(FaultyKeeper zookeeper)
{
    zookeeper->tryCreate(zookeeper_path + "/alive|" + current_host, "", zkutil::CreateMode::Ephemeral);
}

void BackupCoordinationStageSync::readState(FaultyKeeper zookeeper)
{
    zk_nodes_changed->reset();

    /// Get zk nodes and subscribe on their changes.
    Strings zk_nodes = zookeeper->getChildren(zookeeper_path, nullptr, zk_nodes_changed);
    std::unordered_set<std::string_view> zk_nodes_set{zk_nodes.begin(), zk_nodes.end()};

    State new_state;
    {
        std::lock_guard lock{mutex};
        new_state = state;
    }

    /// Read the current state of zk nodes.
    auto error_zk_node = std::find(zk_nodes.begin(), zk_nodes.end(), "error");
    if (error_zk_node != zk_nodes.end())
    {
        String errors = zookeeper->get(zookeeper_path + "/error");
        ReadBufferFromOwnString buf{errors};
        String host;
        readStringBinary(host, buf);
        auto error = readException(buf, fmt::format("Got error from {}", host));

        new_state.error = std::make_pair(host, error);
    }

    auto get_host_info = [&](const String & host) -> HostInfo &
    {
        auto it = new_state.hosts_info.find(host);
        if (it == new_state.hosts_info.end())
        {
            it = new_state.hosts_info.emplace(host, HostInfo{});
            it->second.host = host;
        }
        return it->second;
    };

    new_state.connected_hosts.clear();

    for (const auto & zk_node : zk_nodes)
    {
        if (zk_node.starts_with("started|"))
        {
            String host = zk_node.substr(strlen("started|"));
            auto & host_info = get_host_info(host);
            host_info.started = true;
            continue;
        }

        if (zk_node.starts_with("alive|"))
        {
            String host = zk_node.substr(strlen("alive|"));
            auto & host_info = get_host_info(host);
            host_info.connected = true;
            host_info.last_time_connected = std::chrono::system_clock::now();
            host_info.started = true;
            continue;
        }

        if (zk_node == "error")
        {
            String host_and_exception = zookeeper->get(fs::path{zookeeper_path} / zk_node);
            ReadBufferFromOwnString buf{host_and_exception};
            String host;
            readStringBinary(host, buf);
            auto exception = readException(buf, fmt::format("Got error from {}", host));
            new_state.error = std::make_pair(host, exception);
            continue;
        }

        if (zk_node.starts_with("current|"))
        {
            String host_and_stage = zk_node.substr(strlen("current|"))
            size_t separator_pos = host_and_stage.find('|');
            if (separator_pos == String::npos)
                continue;
            String host = host_and_stage.substr(0, separator_pos);
            String stage = host_and_stage.substr(separator_pos + 1);
            auto & host_info = get_host_info(host);
            if (!host_info.results.contains(stage))
            {
                String stage_result = zookeeper->get(fs::path{zookeeper_path} / zk_node);
                host_info.results[stage] = std::move(stage_result);
            }
        }
    }

    bool was_state_changed;
    {
        std::lock_guard lock{mutex};
        was_state_changed = (new_state != state);
        state = new_state;
    }

    if (was_state_changed)
        state_changed.notify_all();
}


Strings BackupCoordinationStageSync::wait(const Strings & all_hosts, const String & stage_to_wait)
{
    return waitImpl(all_hosts, stage_to_wait, {});
}

Strings BackupCoordinationStageSync::waitFor(const Strings & all_hosts, const String & stage_to_wait, std::chrono::milliseconds timeout)
{
    return waitImpl(all_hosts, stage_to_wait, timeout);
}


Strings BackupCoordinationStageSync::waitImpl(const Strings & all_hosts, const String & stage_to_wait, std::optional<std::chrono::milliseconds> timeout) const
{
    Strings results;
    results.resize(all_hosts.size());

    auto check_if_ready = [&](bool throw_if_not_ready)
    {
        if (state.error)
            state.error->exception.rethrow();

        for (size_t i = 0; i != all_hosts.size(); ++i)
        {
            const String & host = all_hosts[i];
            auto it = state.hosts_info.find(host);
            if (it == state.hosts_info.end())
            {
                if (throw_if_not_ready)
                {
                    throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Never connected to host {}", host);
                }
                else
                {
                    LOG_INFO(log, "No connection to host {} yet", host);
                    return false;
                }
            }

            HostInfo & host_info = it->second;
            auto stage_result_it = host_info.stage_results.find(stage_to_wait);
            if (stage_result_it != host_info.stage_results.end())
            {
                results[i] = stage_result_it;
                continue;
            }

            if (host_info.connected)
            {
                if (throw_if_not_ready)
                    throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Waited for host {} to come to stage {} for too long{}",
                                    host, stage_to_wait, timeout ? fmt::format(" ({})", *timeout) : "");
                else
                    return false;
            }
            else if (host_info.started)
            {
                if (throw_if_not_ready)
                    throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Host disconnected {}{}",
                                    host, host_info.last_time_connected ? fmt::format(", last time it was connected at {}", *host_info.last_time_connected, ""));
                else
                    return false;
            }
            else
            {
                if (throw_if_not_ready)
                    throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Host never started {}", host);
                else
                    return false;
            }
        }
    };

    std::unique_lock lock{mutex};
    if (timeout)
    {
        if (!state_changed.wait_for(lock, [&]() { check_if_ready(/* throw_if_not_ready= */ false); }, timeout))
        {
            check_if_ready(/* throw_if_not_ready= */ true);
        }
    }
    else
    {
        state_changed.wait(lock, [&]() { check_if_ready(/* throw_if_not_ready= */ false); });
    }

    return results;
}



        if (zk_node.starts_with("current|"))
        if (!zk_nodes_set.contains("current|" + host + "|" + stage_to_wait))
        {
            const String started_node_name = "started|" + host;
            const String alive_node_name = "alive|" + host;

            bool started = zk_nodes_set.contains(started_node_name);
            bool alive = zk_nodes_set.contains(alive_node_name);

            if (!alive)
            {
                /// If the "alive" node doesn't exist then we don't have connection to the corresponding host.
                /// This node is ephemeral so probably it will be recreated soon. We use zookeeper retries to wait.
                /// In worst case when we won't manage to see the alive node for a long time we will just abort the backup.
                const auto * const suffix = retries_ctl.isLastRetry() ? "" : ", will retry";
                if (started)
                    retries_ctl.setUserError(Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                                                       "Lost connection to host {}{}", host, suffix));
                else
                    retries_ctl.setUserError(Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                                                       "No connection to host {} yet{}", host, suffix));

                state.disconnected_host = host;
                return state;
            }

            if (!unready_host)
                unready_host.emplace(UnreadyHost{.host = host, .started = started});
        }
    }

    if (unready_host)
    {
        state.unready_host = std::move(unready_host);
        return state;
    }

    Strings results;
    for (const auto & host : all_hosts)
        results.emplace_back(zookeeper->get(zookeeper_path + "/current|" + host + "|" + stage_to_wait));
    state.results = std::move(results);


    state = readCurrentState(holder, zk_nodes, all_hosts, stage_to_wait);
}


void BackupCoordinationStageSync::setStage(const String & new_stage, const String & message)
{
    auto holder = with_retries.createRetriesControlHolder("set");
    holder.retries_ctl.retryLoop(
        [&, &zookeeper = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zookeeper);

        if (all_hosts)
        {
            auto code = zookeeper->trySet(zookeeper_path, new_stage);
            if (code != Coordination::Error::ZOK)
                throw zkutil::KeeperException::fromPath(code, zookeeper_path);
        }
        else
        {
            zookeeper->createIfNotExists(zookeeper_path + "/started|" + current_host, "");
            zookeeper->createIfNotExists(zookeeper_path + "/current|" + current_host + "|" + new_stage, message);
        }
    });
}

void BackupCoordinationStageSync::setError(const String & current_host, const Exception & exception)
{
    auto holder = with_retries.createRetriesControlHolder("setError");
    holder.retries_ctl.retryLoop(
        [&, &zookeeper = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zookeeper);

        WriteBufferFromOwnString buf;
        writeStringBinary(current_host, buf);
        writeException(exception, buf, true);
        zookeeper->createIfNotExists(zookeeper_path + "/error", buf.str());

        /// When backup/restore fails, it removes the nodes from Zookeeper.
        /// Sometimes it fails to remove all nodes. It's possible that it removes /error node, but fails to remove /stage node,
        /// so the following line tries to preserve the error status.
        auto code = zookeeper->trySet(zookeeper_path, Stage::ERROR);
        if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException::fromPath(code, zookeeper_path);
    });
}

Strings BackupCoordinationStageSync::wait(const Strings & all_hosts, const String & stage_to_wait)
{
    return waitImpl(all_hosts, stage_to_wait, {});
}

Strings BackupCoordinationStageSync::waitFor(const Strings & all_hosts, const String & stage_to_wait, std::chrono::milliseconds timeout)
{
    return waitImpl(all_hosts, stage_to_wait, timeout);
}

namespace
{
    struct UnreadyHost
    {
        String host;
        bool started = false;
    };
}

struct BackupCoordinationStageSync::State
{
    std::optional<Strings> results;
    std::optional<std::pair<String, Exception>> error;
    std::optional<String> disconnected_host;
    std::optional<UnreadyHost> unready_host;
};

BackupCoordinationStageSync::State BackupCoordinationStageSync::readCurrentState(
    WithRetries::RetriesControlHolder & retries_control_holder,
    const Strings & zk_nodes,
    const Strings & all_hosts,
    const String & stage_to_wait) const
{
    auto zookeeper = retries_control_holder.faulty_zookeeper;
    auto & retries_ctl = retries_control_holder.retries_ctl;

    std::unordered_set<std::string_view> zk_nodes_set{zk_nodes.begin(), zk_nodes.end()};

    State state;
    if (zk_nodes_set.contains("error"))
    {
        String errors = zookeeper->get(zookeeper_path + "/error");
        ReadBufferFromOwnString buf{errors};
        String host;
        readStringBinary(host, buf);
        state.error = std::make_pair(host, readException(buf, fmt::format("Got error from {}", host)));
        return state;
    }

    std::optional<UnreadyHost> unready_host;

    for (const auto & host : all_hosts)
    {
        if (!zk_nodes_set.contains("current|" + host + "|" + stage_to_wait))
        {
            const String started_node_name = "started|" + host;
            const String alive_node_name = "alive|" + host;

            bool started = zk_nodes_set.contains(started_node_name);
            bool alive = zk_nodes_set.contains(alive_node_name);

            if (!alive)
            {
                /// If the "alive" node doesn't exist then we don't have connection to the corresponding host.
                /// This node is ephemeral so probably it will be recreated soon. We use zookeeper retries to wait.
                /// In worst case when we won't manage to see the alive node for a long time we will just abort the backup.
                const auto * const suffix = retries_ctl.isLastRetry() ? "" : ", will retry";
                if (started)
                    retries_ctl.setUserError(Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                                                       "Lost connection to host {}{}", host, suffix));
                else
                    retries_ctl.setUserError(Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                                                       "No connection to host {} yet{}", host, suffix));

                state.disconnected_host = host;
                return state;
            }

            if (!unready_host)
                unready_host.emplace(UnreadyHost{.host = host, .started = started});
        }
    }

    if (unready_host)
    {
        state.unready_host = std::move(unready_host);
        return state;
    }

    Strings results;
    for (const auto & host : all_hosts)
        results.emplace_back(zookeeper->get(zookeeper_path + "/current|" + host + "|" + stage_to_wait));
    state.results = std::move(results);

    return state;
}

Strings BackupCoordinationStageSync::waitImpl(
    const Strings & all_hosts, const String & stage_to_wait, std::optional<std::chrono::milliseconds> timeout) const
{
    if (all_hosts.empty())
        return {};

    /// Wait until all hosts are ready or an error happens or time is out.

    bool use_timeout = timeout.has_value();
    std::chrono::steady_clock::time_point end_of_timeout;
    if (use_timeout)
        end_of_timeout = std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::steady_clock::duration>(*timeout);

    State state;
    for (;;)
    {
        LOG_INFO(log, "Waiting for the stage {}", stage_to_wait);
        /// Set by ZooKepper when list of zk nodes have changed.
        auto watch = std::make_shared<Poco::Event>();
        Strings zk_nodes;
        {
            auto holder = with_retries.createRetriesControlHolder("waitImpl");
            holder.retries_ctl.retryLoop(
                [&, &zookeeper = holder.faulty_zookeeper]()
            {
                with_retries.renewZooKeeper(zookeeper);
                watch->reset();
                /// Get zk nodes and subscribe on their changes.
                zk_nodes = zookeeper->getChildren(zookeeper_path, nullptr, watch);

                /// Read the current state of zk nodes.
                state = readCurrentState(holder, zk_nodes, all_hosts, stage_to_wait);
            });
        }

        /// Analyze the current state of zk nodes.
        chassert(state.results || state.error || state.disconnected_host || state.unready_host);

        if (state.results || state.error || state.disconnected_host)
            break; /// Everything is ready or error happened.

        /// Log what we will wait.
        const auto & unready_host = *state.unready_host;
        LOG_INFO(log, "Waiting on ZooKeeper watch for any node to be changed (currently waiting for host {}{})",
                 unready_host.host,
                 (!unready_host.started ? " which didn't start the operation yet" : ""));

        /// Wait until `watch_callback` is called by ZooKeeper meaning that zk nodes have changed.
        {
            if (use_timeout)
            {
                auto current_time = std::chrono::steady_clock::now();
                if ((current_time > end_of_timeout)
                    || !watch->tryWait(std::chrono::duration_cast<std::chrono::milliseconds>(end_of_timeout - current_time).count()))
                    break;
            }
            else
            {
                watch->wait();
            }
        }
    }

    /// Rethrow an error raised originally on another host.
    if (state.error)
        state.error->second.rethrow();

    /// Another host terminated without errors.
    if (state.disconnected_host)
        throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "No connection to host {}", *state.disconnected_host);

    /// Something's unready, timeout is probably not enough.
    if (state.unready_host)
    {
        const auto & unready_host = *state.unready_host;
        throw Exception(
            ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
            "Waited for host {} too long (> {}){}",
            unready_host.host,
            to_string(*timeout),
            unready_host.started ? "" : ": Operation didn't start");
    }

    LOG_TRACE(log, "Everything is Ok. All hosts achieved stage {}", stage_to_wait);
    return std::move(*state.results);
}

}
