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
    Poco::Logger * log_)
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
            /// Make an ephemeral node so the initiator can track if the current host is still working.
            String alive_node_path = zookeeper_path + "/alive|" + current_host;
            auto code = zookeeper->tryCreate(alive_node_path, "", zkutil::CreateMode::Ephemeral);
            if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
                throw zkutil::KeeperException::fromPath(code, alive_node_path);

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
    struct UnreadyHostState
    {
        bool started = false;
        bool alive = false;
    };
}

struct BackupCoordinationStageSync::State
{
    Strings results;
    std::map<String, UnreadyHostState> unready_hosts;
    std::optional<std::pair<String, Exception>> error;
    std::optional<String> host_terminated;
};

BackupCoordinationStageSync::State BackupCoordinationStageSync::readCurrentState(
    const Strings & zk_nodes, const Strings & all_hosts, const String & stage_to_wait) const
{
    std::unordered_set<std::string_view> zk_nodes_set{zk_nodes.begin(), zk_nodes.end()};

    State state;
    if (zk_nodes_set.contains("error"))
    {
        String errors;
        {
            auto holder = with_retries.createRetriesControlHolder("readCurrentState");
            holder.retries_ctl.retryLoop(
                [&, &zookeeper = holder.faulty_zookeeper]()
                {
                    with_retries.renewZooKeeper(zookeeper);
                    errors = zookeeper->get(zookeeper_path + "/error");
                });
        }
        ReadBufferFromOwnString buf{errors};
        String host;
        readStringBinary(host, buf);
        state.error = std::make_pair(host, readException(buf, fmt::format("Got error from {}", host)));
        return state;
    }

    for (const auto & host : all_hosts)
    {
        if (!zk_nodes_set.contains("current|" + host + "|" + stage_to_wait))
        {
            UnreadyHostState unready_host_state;
            const String started_node_name = "started|" + host;
            const String alive_node_name = "alive|" + host;
            const String alive_node_path = zookeeper_path + "/" + alive_node_name;
            unready_host_state.started = zk_nodes_set.contains(started_node_name);

            /// Because we do retries everywhere we can't fully rely on ephemeral nodes anymore.
            /// Though we recreate "alive" node when reconnecting it might be not enough and race condition is possible.
            /// And everything we can do here - just retry.
            /// In worst case when we won't manage to see the alive node for a long time we will just abort the backup.
            unready_host_state.alive = zk_nodes_set.contains(alive_node_name);
            if (!unready_host_state.alive)
            {
                LOG_TRACE(log, "Seems like host ({}) is dead. Will retry the check to confirm", host);
                auto holder = with_retries.createRetriesControlHolder("readCurrentState::checkAliveNode");
                holder.retries_ctl.retryLoop(
                    [&, &zookeeper = holder.faulty_zookeeper]()
                {
                    with_retries.renewZooKeeper(zookeeper);

                    if (zookeeper->existsNoFailureInjection(alive_node_path))
                    {
                        unready_host_state.alive = true;
                        return;
                    }

                    // Retry with backoff. We also check whether it is last retry or no, because we won't to rethrow an exception.
                    if (!holder.retries_ctl.isLastRetry())
                        holder.retries_ctl.setKeeperError(Coordination::Error::ZNONODE, "There is no alive node for host {}. Will retry", host);
                });
            }
            LOG_TRACE(log, "Host ({}) appeared to be {}", host, unready_host_state.alive ? "alive" : "dead");

            state.unready_hosts.emplace(host, unready_host_state);
            if (!unready_host_state.alive && unready_host_state.started && !state.host_terminated)
                state.host_terminated = host;
        }
    }

    if (state.host_terminated || !state.unready_hosts.empty())
        return state;

    auto holder = with_retries.createRetriesControlHolder("waitImpl::collectStagesToWait");
    holder.retries_ctl.retryLoop(
        [&, &zookeeper = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zookeeper);
        Strings results;

        for (const auto & host : all_hosts)
            results.emplace_back(zookeeper->get(zookeeper_path + "/current|" + host + "|" + stage_to_wait));

        state.results = std::move(results);
    });

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
            auto holder = with_retries.createRetriesControlHolder("waitImpl::getChildren");
            holder.retries_ctl.retryLoop(
                [&, &zookeeper = holder.faulty_zookeeper]()
            {
                with_retries.renewZooKeeper(zookeeper);
                watch->reset();
                /// Get zk nodes and subscribe on their changes.
                zk_nodes = zookeeper->getChildren(zookeeper_path, nullptr, watch);
            });
        }

        /// Read and analyze the current state of zk nodes.
        state = readCurrentState(zk_nodes, all_hosts, stage_to_wait);
        if (state.error || state.host_terminated || state.unready_hosts.empty())
            break; /// Error happened or everything is ready.

        /// Log that we will wait
        const auto & unready_host = state.unready_hosts.begin()->first;
        LOG_INFO(log, "Waiting on ZooKeeper watch for any node to be changed (currently waiting for host {})", unready_host);

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
    if (state.host_terminated)
        throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Host {} suddenly stopped working", *state.host_terminated);

    /// Something's unready, timeout is probably not enough.
    if (!state.unready_hosts.empty())
    {
        const auto & [unready_host, unready_host_state] = *state.unready_hosts.begin();
        throw Exception(
            ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
            "Waited for host {} too long (> {}){}",
            unready_host,
            to_string(*timeout),
            unready_host_state.started ? "" : ": Operation didn't start");
    }

    LOG_TRACE(log, "Everything is Ok. All hosts achieved stage {}", stage_to_wait);
    return state.results;
}

}
