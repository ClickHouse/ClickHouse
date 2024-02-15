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
    createRootNodes();
}

void BackupCoordinationStageSync::createRootNodes()
{
    auto holder = with_retries.createRetriesControlHolder("createRootNodes");
    holder.retries_ctl.retryLoop(
        [&, &zookeeper = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zookeeper);
        zookeeper->createAncestors(zookeeper_path);
        zookeeper->createIfNotExists(zookeeper_path, "");
    });
}

void BackupCoordinationStageSync::set(const String & current_host, const String & new_stage, const String & message, const bool & all_hosts)
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
