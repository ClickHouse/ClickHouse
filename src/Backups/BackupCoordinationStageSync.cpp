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
#include <Backups/BackupConcurrencyCheck.h>
#include <Poco/URI.h>
#include <boost/algorithm/string/join.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int FAILED_TO_SYNC_BACKUP_OR_RESTORE;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
}

namespace
{
    /// The coordination version is stored in the 'start' node for each host
    /// by each host when it starts working on this backup or restore.
    enum Version
    {
        kInitialVersion = 1,

        /// This old version didn't create the 'finish' node, it uses stage "completed" to tell other hosts that the work is done.
        /// If an error happened this old version didn't change any nodes to tell other hosts that the error handling is done.
        /// So while using this old version hosts couldn't know when other hosts are done with the error handling,
        /// and that situation caused weird errors in the logs somehow.
        /// Also this old version didn't create the 'start' node for the initiator.
        kVersionWithoutFinishNode = 1,

        /// Now we create the 'finish' node both if the work is done or if the error handling is done.

        kCurrentVersion = 2,
    };
}

bool BackupCoordinationStageSync::HostInfo::operator ==(const HostInfo & other) const
{
    /// We don't compare `last_connection_time` here.
    return (host == other.host) && (started == other.started) && (connected == other.connected) && (finished == other.finished)
        && (stages == other.stages) && (!!exception == !!other.exception);
}

bool BackupCoordinationStageSync::HostInfo::operator !=(const HostInfo & other) const
{
    return !(*this == other);
}

bool BackupCoordinationStageSync::State::operator ==(const State & other) const = default;
bool BackupCoordinationStageSync::State::operator !=(const State & other) const = default;


void BackupCoordinationStageSync::State::merge(const State & other)
{
    if (other.host_with_error)
    {
        const String & host = *other.host_with_error;
        addErrorInfo(other.hosts.at(host).exception, host);
    }

    for (const auto & [host, other_host_info] : other.hosts)
    {
        auto & host_info = hosts.at(host);
        host_info.stages.insert(other_host_info.stages.begin(), other_host_info.stages.end());
        if (other_host_info.finished)
            host_info.finished = true;
    }
}


void BackupCoordinationStageSync::State::addErrorInfo(std::exception_ptr exception, const String & host)
{
    if (!host_with_error && exception)
    {
        host_with_error = host;
        hosts.at(host).exception = exception;
    }
}


BackupCoordinationStageSync::BackupCoordinationStageSync(
        bool is_restore_,
        const String & zookeeper_path_,
        const String & current_host_,
        const Strings & all_hosts_,
        bool allow_concurrency_,
        BackupConcurrencyCounters & concurrency_counters_,
        const WithRetries & with_retries_,
        ThreadPoolCallbackRunnerUnsafe<void> schedule_,
        QueryStatusPtr process_list_element_,
        LoggerPtr log_)
    : is_restore(is_restore_)
    , operation_name(is_restore ? "restore" : "backup")
    , current_host(current_host_)
    , current_host_desc(getHostDesc(current_host))
    , all_hosts(all_hosts_)
    , allow_concurrency(allow_concurrency_)
    , concurrency_counters(concurrency_counters_)
    , with_retries(with_retries_)
    , schedule(schedule_)
    , process_list_element(process_list_element_)
    , log(log_)
    , failure_after_host_disconnected_for_seconds(with_retries.getKeeperSettings().failure_after_host_disconnected_for_seconds)
    , finish_timeout_after_error(with_retries.getKeeperSettings().finish_timeout_after_error)
    , sync_period_ms(with_retries.getKeeperSettings().sync_period_ms)
    // all_hosts.size() is added to max_attempts_after_bad_version since each host change the num_hosts node once, and it's a valid case
    , max_attempts_after_bad_version(with_retries.getKeeperSettings().max_attempts_after_bad_version + all_hosts.size())
    , zookeeper_path(zookeeper_path_)
    , root_zookeeper_path(zookeeper_path.parent_path().parent_path())
    , operation_zookeeper_path(zookeeper_path.parent_path())
    , operation_node_name(zookeeper_path.parent_path().filename())
    , start_node_path(zookeeper_path / ("started|" + current_host))
    , finish_node_path(zookeeper_path / ("finished|" + current_host))
    , initiator_start_node_path(zookeeper_path / ("started|" + String{kInitiator}))
    , num_hosts_node_path(zookeeper_path / "num_hosts")
    , error_node_path(zookeeper_path / "error")
    , alive_node_path(zookeeper_path / ("alive|" + current_host))
    , alive_tracker_node_path(fs::path{root_zookeeper_path} / "alive_tracker")
    , zk_nodes_changed(std::make_shared<Poco::Event>())
{
    initializeState();
}


BackupCoordinationStageSync::~BackupCoordinationStageSync()
{
    /// If everything is ok, then the finish() function should be called already and the watching thread should be already stopped too.
    /// However if an error happened then that might be different,
    /// so here in the destructor we need to ensure that we've tried to create the finish node and also we've stopped the watching thread.

    if (!finished())
        finish(/* throw_if_error = */ false);

    stopWatchingThread();
}


void BackupCoordinationStageSync::initializeState()
{
    std::lock_guard lock{mutex};
    auto now = std::chrono::system_clock::now();
    auto monotonic_now = std::chrono::steady_clock::now();

    for (const String & host : all_hosts)
        state.hosts.emplace(host, HostInfo{.host = host, .last_connection_time = now, .last_connection_time_monotonic = monotonic_now});

    if (!state.hosts.contains(current_host))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "List of hosts must contain the current host");

    if (!state.hosts.contains(String{kInitiator}))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "List of hosts must contain the initiator");

    /// We know the version of the current host.
    state.hosts.at(current_host).version = kCurrentVersion;
}


String BackupCoordinationStageSync::getHostDesc(const String & host)
{
    String res;
    if (host.empty())
    {
        res = "the initiator";
    }
    else
    {
        try
        {
            res = "host ";
            Poco::URI::decode(host, res); /// Append the decoded host name to `res`.
        }
        catch (const Poco::URISyntaxException &)
        {
            res = "host " + host;
        }
    }
    return res;
}


String BackupCoordinationStageSync::getHostsDesc(const Strings & hosts)
{
    String res = "[";
    for (const String & host : hosts)
    {
        if (res != "[")
            res += ", ";
        res += getHostDesc(host);
    }
    res += "]";
    return res;
}


void BackupCoordinationStageSync::startup()
{
    createRootNodes();
    createStartAndAliveNodesAndCheckConcurrency();
    readInitiatorVersion();
    startWatchingThread();
}


void BackupCoordinationStageSync::createRootNodes()
{
    if ((zookeeper_path.filename() != "stage") || !operation_node_name.starts_with(is_restore ? "restore-" : "backup-")
        || (root_zookeeper_path == operation_zookeeper_path))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected path in ZooKeeper specified: {}", zookeeper_path);
    }

    auto holder = with_retries.createRetriesControlHolder("BackupCoordinationStageSync::createRootNodes", WithRetries::kInitialization);
    holder.retries_ctl.retryLoop(
        [&, &zookeeper = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zookeeper);
            zookeeper->createAncestors(root_zookeeper_path);
            zookeeper->createIfNotExists(root_zookeeper_path, "");
        });
}


void BackupCoordinationStageSync::createStartAndAliveNodesAndCheckConcurrency()
{
    auto holder = with_retries.createRetriesControlHolder("BackupCoordinationStageSync::createStartAndAliveNodes", WithRetries::kInitialization);
    holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zookeeper);
        createStartAndAliveNodesAndCheckConcurrency(zookeeper);
    });

    /// The local concurrency check should be done here after BackupCoordinationStageSync::checkConcurrency() checked that
    /// there are no 'alive' nodes corresponding to other backups or restores.
    local_concurrency_check.emplace(is_restore, /* on_cluster = */ true, zookeeper_path, allow_concurrency, concurrency_counters);
}


void BackupCoordinationStageSync::createStartAndAliveNodesAndCheckConcurrency(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper)
{
    /// The "num_hosts" node keeps the number of hosts which started (created the "started" node)
    /// but not yet finished (not created the "finished" node).
    /// The number of alive hosts can be less than that.

    /// The "alive_tracker" node always keeps an empty string, we track its version only.
    /// The "alive_tracker" node increases its version each time when any "alive" nodes are created
    /// so we use it to check concurrent backups/restores.
    zookeeper->createIfNotExists(alive_tracker_node_path, "");

    std::optional<size_t> num_hosts;
    int num_hosts_version = -1;

    bool check_concurrency = !allow_concurrency;
    int alive_tracker_version = -1;

    for (size_t attempt_no = 1; attempt_no <= max_attempts_after_bad_version; ++attempt_no)
    {
        if (!num_hosts)
        {
            String num_hosts_str;
            Coordination::Stat stat;
            if (zookeeper->tryGet(num_hosts_node_path, num_hosts_str, &stat))
            {
                num_hosts = parseFromString<size_t>(num_hosts_str);
                num_hosts_version = stat.version;
            }
        }

        if (check_concurrency)
        {
            Coordination::Stat stat;
            zookeeper->exists(alive_tracker_node_path, &stat);
            alive_tracker_version = stat.version;

            checkConcurrency(zookeeper);
            check_concurrency = false;
        }

        if (zookeeper->exists(start_node_path))
        {
            /// The "start" node for the current host already exists.
            /// That can happen if previous attempt failed because of a connection loss but was in fact successful.
            LOG_INFO(log, "The start node in ZooKeeper for {} already exists", current_host_desc);
            return;
        }

        Coordination::Requests requests;
        requests.reserve(6);

        size_t operation_node_pos = static_cast<size_t>(-1);
        if (!zookeeper->exists(operation_zookeeper_path))
        {
            operation_node_pos = requests.size();
            requests.emplace_back(zkutil::makeCreateRequest(operation_zookeeper_path, "", zkutil::CreateMode::Persistent));
        }

        size_t zookeeper_path_pos = static_cast<size_t>(-1);
        if (!zookeeper->exists(zookeeper_path))
        {
            zookeeper_path_pos = requests.size();
            requests.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));
        }

        size_t num_hosts_node_pos = requests.size();
        if (num_hosts)
            requests.emplace_back(zkutil::makeSetRequest(num_hosts_node_path, toString(*num_hosts + 1), num_hosts_version));
        else
            requests.emplace_back(zkutil::makeCreateRequest(num_hosts_node_path, "1", zkutil::CreateMode::Persistent));

        size_t alive_tracker_node_pos = requests.size();
        requests.emplace_back(zkutil::makeSetRequest(alive_tracker_node_path, "", alive_tracker_version));

        requests.emplace_back(zkutil::makeCreateRequest(start_node_path, std::to_string(kCurrentVersion), zkutil::CreateMode::Persistent));
        requests.emplace_back(zkutil::makeCreateRequest(alive_node_path, "", zkutil::CreateMode::Ephemeral));

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(requests, responses);

        if (code == Coordination::Error::ZOK)
        {
            LOG_INFO(log, "Created start node #{} in ZooKeeper for {} (coordination version: {})",
                     num_hosts.value_or(0) + 1, current_host_desc, static_cast<int>(kCurrentVersion));
            return;
        }

        auto show_error_before_next_attempt = [&](const String & message)
        {
            bool will_try_again = (attempt_no < max_attempts_after_bad_version);
            LOG_TRACE(log, "{} (attempt #{}){}", message, attempt_no, will_try_again ? ", will try again" : "");
            if (!will_try_again)
            {
                throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                                "Couldn't create the start node in ZooKeeper for {} after {} attempts: {}", current_host_desc, attempt_no, message);
            }
        };

        if ((operation_node_pos < responses.size()) &&
            (responses[operation_node_pos]->error == Coordination::Error::ZNODEEXISTS))
        {
            show_error_before_next_attempt(fmt::format("Node {} already exists", operation_zookeeper_path));
            /// needs another attempt
        }
        else if ((zookeeper_path_pos < responses.size()) &&
            (responses[zookeeper_path_pos]->error == Coordination::Error::ZNODEEXISTS))
        {
            show_error_before_next_attempt(fmt::format("Node {} already exists", zookeeper_path));
            /// needs another attempt
        }
        else if ((num_hosts_node_pos < responses.size()) && !num_hosts &&
            (responses[num_hosts_node_pos]->error == Coordination::Error::ZNODEEXISTS))
        {
            show_error_before_next_attempt(fmt::format("Node {} already exists", num_hosts_node_path));
            /// needs another attempt
        }
        else if ((num_hosts_node_pos < responses.size()) && num_hosts &&
            (responses[num_hosts_node_pos]->error == Coordination::Error::ZBADVERSION))
        {
            show_error_before_next_attempt(fmt::format("The version of node {} changed", num_hosts_node_path));
            num_hosts.reset(); /// needs to reread 'num_hosts' again
        }
        else if ((alive_tracker_node_pos < responses.size()) &&
            (responses[alive_tracker_node_pos]->error == Coordination::Error::ZBADVERSION))
        {
            show_error_before_next_attempt(fmt::format("The version of node {} changed", alive_tracker_node_path));
            check_concurrency = true; /// needs to recheck for concurrency again
        }
        else
        {
            zkutil::KeeperMultiException::check(code, requests, responses);
        }
    }

    UNREACHABLE();
}


void BackupCoordinationStageSync::checkConcurrency(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper)
{
    if (allow_concurrency)
        return;

    Strings found_operations;
    auto code = zookeeper->tryGetChildren(root_zookeeper_path, found_operations);

    if (!((code == Coordination::Error::ZOK) || (code == Coordination::Error::ZNONODE)))
        throw zkutil::KeeperException::fromPath(code, root_zookeeper_path);

    if (code == Coordination::Error::ZNONODE)
        return;

    for (const String & found_operation : found_operations)
    {
        if (found_operation.starts_with(is_restore ? "restore-" : "backup-") && (found_operation != operation_node_name))
        {
            Strings stages;
            code = zookeeper->tryGetChildren(fs::path{root_zookeeper_path} / found_operation / "stage", stages);

            if (!((code == Coordination::Error::ZOK) || (code == Coordination::Error::ZNONODE)))
                throw zkutil::KeeperException::fromPath(code, fs::path{root_zookeeper_path} / found_operation / "stage");

            if (code == Coordination::Error::ZOK)
            {
                for (const String & stage : stages)
                {
                    if (stage.starts_with("alive"))
                        BackupConcurrencyCheck::throwConcurrentOperationNotAllowed(is_restore);
                }
            }
        }
    }
}


void BackupCoordinationStageSync::readInitiatorVersion()
{
    if (current_host == kInitiator)
    {
        chassert(getInitiatorVersion() == kCurrentVersion);
        return;
    }

    auto holder = with_retries.createRetriesControlHolder("BackupCoordinationStageSync::readInitiatorVersion", WithRetries::kInitialization);
    holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zookeeper);
        String initiator_start_node;
        if (!zookeeper->tryGet(initiator_start_node_path, initiator_start_node))
        {
            LOG_TRACE(log, "Couldn't read the initiator's version, assuming {}", kInitialVersion);
        }
        std::lock_guard lock{mutex};
        state.hosts.at(String{kInitiator}).version = parseStartNode(initiator_start_node, String{kInitiator});
    });
}


void BackupCoordinationStageSync::startWatchingThread()
{
    watching_thread_future = schedule([this]() { watchingThread(); }, Priority{});
}


void BackupCoordinationStageSync::stopWatchingThread()
{
    {
        std::lock_guard lock{mutex};
        if (should_stop_watching_thread)
            return;
        should_stop_watching_thread = true;

        /// Wake up waiting threads.
        if (zk_nodes_changed)
            zk_nodes_changed->set();
        state_changed.notify_all();
    }

    if (watching_thread_future.valid())
        watching_thread_future.wait();

    LOG_TRACE(log, "Stopped the watching thread");
}


void BackupCoordinationStageSync::watchingThread()
{
    LOG_TRACE(log, "Started the watching thread");

    auto component_guard = Coordination::setCurrentComponent("BackupCoordinationStageSync::watchingThread");
    auto should_stop = [&]
    {
        std::lock_guard lock{mutex};
        return should_stop_watching_thread;
    };

    while (!should_stop())
    {
        try
        {
            /// Check if the current BACKUP or RESTORE command is already cancelled.
            checkIfQueryCancelled();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caugth exception while watching");
        }

        try
        {
            /// Recreate the 'alive' node if necessary and read a new state from ZooKeeper.
            auto holder = with_retries.createRetriesControlHolder("BackupCoordinationStageSync::watchingThread");
            auto & zookeeper = holder.faulty_zookeeper;
            with_retries.renewZooKeeper(zookeeper);

            if (should_stop())
                return;

            /// Recreate the 'alive' node if it was removed.
            createAliveNode(zookeeper);

            /// Reads the current state from nodes in ZooKeeper.
            readCurrentState(zookeeper);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caught exception while watching");

            /// Reset the `connected` flag for each host, we'll set them to true again after we find the 'alive' nodes.
            resetConnectedFlag();
        }

        try
        {
            /// Cancel the query if there is an error on another host or if some host was disconnected too long.
            cancelQueryIfError();
            cancelQueryIfDisconnectedTooLong();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caught exception while watching");
        }

        if (should_stop())
            return;

        zk_nodes_changed->tryWait(sync_period_ms.count());
    }
}


void BackupCoordinationStageSync::createAliveNode(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper)
{
    if (zookeeper->exists(alive_node_path))
        return;

    Coordination::Requests requests;
    requests.emplace_back(zkutil::makeCreateRequest(alive_node_path, "", zkutil::CreateMode::Ephemeral));
    requests.emplace_back(zkutil::makeSetRequest(alive_tracker_node_path, "", -1));
    zookeeper->multi(requests);

    LOG_INFO(log, "The alive node was recreated for {}", current_host_desc);
}


void BackupCoordinationStageSync::resetConnectedFlag()
{
    std::lock_guard lock{mutex};
    auto monotonic_now = std::chrono::steady_clock::now();
    auto now = std::chrono::system_clock::now();
    for (auto & [_, host_info] : state.hosts)
    {
        /// Update the last connection time only for hosts that were previously connected.
        /// This ensures the disconnection timer in cancelQueryIfDisconnectedTooLong() starts
        /// from the moment we first detected the issue (i.e., when we could no longer read ZooKeeper),
        /// rather than from the time of the last successful readCurrentState() call.
        /// Without this, when the initiator itself loses its ZooKeeper connection,
        /// resetConnectedFlag() marks all hosts as disconnected but their last_connection_time
        /// remains stale (from the previous sync cycle), which can cause
        /// cancelQueryIfDisconnectedTooLong() to trigger immediately if sync_period_ms
        /// exceeds failure_after_host_disconnected_for_seconds.
        if (host_info.connected)
        {
            host_info.last_connection_time = now;
            host_info.last_connection_time_monotonic = monotonic_now;
        }
        host_info.connected = false;
    }
}


void BackupCoordinationStageSync::readCurrentState(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper)
{
    (*zk_nodes_changed).reset();

    /// Get zk nodes and subscribe on their changes.
    Strings new_zk_nodes = zookeeper->getChildren(zookeeper_path, nullptr, zk_nodes_changed);
    std::sort(new_zk_nodes.begin(), new_zk_nodes.end()); /// Sorting is necessary because we compare the list of zk nodes with its previous versions.

    State new_state;

    {
        std::lock_guard lock{mutex};

        /// Log all changes in zookeeper nodes in the "stage" folder to make debugging easier.
        Strings added_zk_nodes;
        Strings removed_zk_nodes;
        std::set_difference(new_zk_nodes.begin(), new_zk_nodes.end(), zk_nodes.begin(), zk_nodes.end(), back_inserter(added_zk_nodes));
        std::set_difference(zk_nodes.begin(), zk_nodes.end(), new_zk_nodes.begin(), new_zk_nodes.end(), back_inserter(removed_zk_nodes));
        if (!added_zk_nodes.empty())
            LOG_TRACE(log, "Detected new zookeeper nodes appeared in the stage folder: {}", boost::algorithm::join(added_zk_nodes, ", "));
        if (!removed_zk_nodes.empty())
            LOG_TRACE(log, "Detected that some zookeeper nodes disappeared from the stage folder: {}", boost::algorithm::join(removed_zk_nodes, ", "));

        zk_nodes = new_zk_nodes;
        new_state = state;
        for (auto & [_, host_info] : new_state.hosts)
            host_info.connected = false;
    }

    auto get_host_info = [&](const String & host) -> HostInfo *
    {
        auto it = new_state.hosts.find(host);
        if (it == new_state.hosts.end())
            return nullptr;
        return &it->second;
    };

    auto now = std::chrono::system_clock::now();
    auto monotonic_now = std::chrono::steady_clock::now();

    /// Read the current state from zookeeper nodes.
    /// First we process the "started" nodes because we need to read versions from them.
    for (const auto & zk_node : new_zk_nodes)
    {
        if (zk_node.starts_with("started|"))
        {
            String host = zk_node.substr(strlen("started|"));
            if (auto * host_info = get_host_info(host))
            {
                if (!host_info->started)
                {
                    host_info->version = parseStartNode(zookeeper->get(zookeeper_path / zk_node), host);
                    host_info->started = true;
                }
            }
        }
    }

    for (const auto & zk_node : new_zk_nodes)
    {
        if (zk_node == "error")
        {
            if (!new_state.host_with_error)
            {
                String serialized_error = zookeeper->get(error_node_path);
                auto [exception, host] = parseErrorNode(serialized_error);
                if (exception)
                    new_state.addErrorInfo(exception, host);
            }
        }
        else if (zk_node.starts_with("finished|"))
        {
            String host = zk_node.substr(strlen("finished|"));
            if (auto * host_info = get_host_info(host))
                host_info->finished = true;
        }
        else if (zk_node.starts_with("alive|"))
        {
            String host = zk_node.substr(strlen("alive|"));
            if (auto * host_info = get_host_info(host))
            {
                host_info->connected = true;
                host_info->last_connection_time = now;
                host_info->last_connection_time_monotonic = monotonic_now;
            }
        }
        else if (zk_node.starts_with("current|"))
        {
            String host_and_stage = zk_node.substr(strlen("current|"));
            size_t separator_pos = host_and_stage.find('|');
            if (separator_pos != String::npos)
            {
                String host = host_and_stage.substr(0, separator_pos);
                String stage = host_and_stage.substr(separator_pos + 1);
                if (auto * host_info = get_host_info(host))
                {
                    String result = zookeeper->get(fs::path{zookeeper_path} / zk_node);
                    host_info->stages[stage] = std::move(result);

                    /// That old version didn't create the 'finish' node so we consider that a host finished its work
                    /// if it reached the "completed" stage.
                    if ((host_info->version == kVersionWithoutFinishNode) && (stage == BackupCoordinationStage::COMPLETED))
                        host_info->finished = true;
                }
            }
        }
    }

    /// Check if the state has been just changed, and if so then wake up waiting threads (see waitHostsReachStage()).
    bool was_state_changed = false;

    {
        std::lock_guard lock{mutex};
        /// We were reading `new_state` from ZooKeeper with `mutex` unlocked, so `state` could get more information during that reading,
        /// we don't want to lose that information, that's why we use merge() here.
        new_state.merge(state);
        was_state_changed = (new_state != state);
        state = std::move(new_state);
    }

    if (was_state_changed)
        state_changed.notify_all();
}


int BackupCoordinationStageSync::parseStartNode(const String & start_node_contents, const String & host) const
{
    int version;
    if (start_node_contents.empty())
    {
        version = kInitialVersion;
    }
    else if (!tryParse(version, start_node_contents) || (version < kInitialVersion))
    {
        throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                        "Coordination version {} used by {} is not supported", start_node_contents, getHostDesc(host));
    }

    if (version < kCurrentVersion)
        LOG_WARNING(log, "Coordination version {} used by {} is outdated", version, getHostDesc(host));
    return version;
}


void BackupCoordinationStageSync::checkIfQueryCancelled()
{
    if (process_list_element->checkTimeLimitSoft())
        return; /// Not cancelled.
    state_changed.notify_all();
}


void BackupCoordinationStageSync::cancelQueryIfError()
{
    std::exception_ptr exception;

    {
        std::lock_guard lock{mutex};
        if (state.host_with_error)
            exception = state.hosts.at(*state.host_with_error).exception;
    }

    if (!exception)
        return;

    auto code = getExceptionErrorCode(exception);
    auto cancel_reason = ((code == ErrorCodes::QUERY_WAS_CANCELLED) || (code == ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT))
        ? CancelReason::CANCELLED_BY_USER
        : CancelReason::CANCELLED_BY_ERROR;

    process_list_element->cancelQuery(cancel_reason, exception);

    state_changed.notify_all();
}


void BackupCoordinationStageSync::cancelQueryIfDisconnectedTooLong()
{
    std::exception_ptr exception;

    {
        std::lock_guard lock{mutex};
        if (state.host_with_error || ((failure_after_host_disconnected_for_seconds.count() == 0)))
            return;

        auto monotonic_now = std::chrono::steady_clock::now();
        bool info_shown = false;

        for (auto & [host, host_info] : state.hosts)
        {
            if (!host_info.connected && !host_info.finished && (host != current_host))
            {
                auto disconnected_duration = std::chrono::duration_cast<std::chrono::seconds>(monotonic_now - host_info.last_connection_time_monotonic);
                if (disconnected_duration > failure_after_host_disconnected_for_seconds)
                {
                    /// Host `host` was disconnected too long.
                    /// We can't just throw an exception here because readCurrentState() is called from a background thread.
                    /// So here we're writingh the error to the `process_list_element` and let it to be thrown later
                    /// from `process_list_element->checkTimeLimit()`.
                    String message = fmt::format("The 'alive' node hasn't been updated in ZooKeeper for {} for {} "
                                                 "which is more than the specified timeout {}. Last time the 'alive' node was detected at {}",
                                                 getHostDesc(host), disconnected_duration, failure_after_host_disconnected_for_seconds,
                                                 host_info.last_connection_time);
                    LOG_WARNING(log, "Lost connection to {}: {}", getHostDesc(host), message);
                    exception = std::make_exception_ptr(Exception{ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Lost connection to {}: {}", getHostDesc(host), message});
                    break;
                }

                if ((disconnected_duration >= std::chrono::seconds{1}) && !info_shown)
                {
                    LOG_TRACE(log, "The 'alive' node hasn't been updated in ZooKeeper for {} for {}", getHostDesc(host), disconnected_duration);
                    info_shown = true;
                }
            }
        }
    }

    if (!exception)
        return;

    /// In this function we only pass the new `exception` (about that the connection was lost) to `process_list_element`.
    /// We don't try to create the 'error' node here (because this function is called from watchingThread() and
    /// we don't want the watching thread to try waiting here for retries or a reconnection).
    /// Also we don't set the `state.host_with_error` field here because `state.host_with_error` can only be set
    /// AFTER creating the 'error' node (see the comment for `State`).
    process_list_element->cancelQuery(CancelReason::CANCELLED_BY_ERROR, exception);

    state_changed.notify_all();
}


void BackupCoordinationStageSync::setQueryIsSentToOtherHosts()
{
    std::lock_guard lock{mutex};
    query_is_sent_to_other_hosts = true;
}

bool BackupCoordinationStageSync::isQuerySentToOtherHosts() const
{
    std::lock_guard lock{mutex};
    return query_is_sent_to_other_hosts;
}


void BackupCoordinationStageSync::setStage(const String & stage, const String & stage_result)
{
    LOG_INFO(log, "{} reached stage {}", current_host_desc, stage);

    {
        std::lock_guard lock{mutex};
        if (state.hosts.at(current_host).stages.contains(stage))
            return; /// Already set.
    }

    if ((getInitiatorVersion() == kVersionWithoutFinishNode) && (stage == BackupCoordinationStage::COMPLETED))
    {
        LOG_TRACE(log, "Stopping the watching thread because the initiator uses outdated version {}", getInitiatorVersion());
        stopWatchingThread();
    }

    auto holder = with_retries.createRetriesControlHolder("BackupCoordinationStageSync::setStage");
    holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zookeeper);
        createStageNode(stage, stage_result, zookeeper);
    });

    /// If the initiator of the query has that old version then it doesn't expect us to create the 'finish' node and moreover
    /// the initiator can start removing all the nodes immediately after all hosts report about reaching the "completed" status.
    /// So to avoid weird errors in the logs we won't create the 'finish' node if the initiator of the query has that old version.
    if ((getInitiatorVersion() == kVersionWithoutFinishNode) && (stage == BackupCoordinationStage::COMPLETED))
    {
        LOG_INFO(log, "Skipped creating the 'finish' node because the initiator uses outdated version {}", getInitiatorVersion());
        std::lock_guard lock{mutex};
        state.hosts.at(current_host).finished = true;
    }
}


void BackupCoordinationStageSync::createStageNode(const String & stage, const String & stage_result, Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper)
{
    if (isErrorSet())
        rethrowSetError();

    String serialized_error;
    if (zookeeper->tryGet(error_node_path, serialized_error))
    {
        auto [exception, host] = parseErrorNode(serialized_error);
        if (exception)
        {
            std::lock_guard lock{mutex};
            state.addErrorInfo(exception, host);
            std::rethrow_exception(exception);
        }
    }

    auto code = zookeeper->tryCreate(getStageNodePath(stage), stage_result, zkutil::CreateMode::Persistent);
    if (code == Coordination::Error::ZOK)
    {
        std::lock_guard lock{mutex};
        state.hosts.at(current_host).stages[stage] = stage_result;
        return;
    }

    if (code == Coordination::Error::ZNODEEXISTS)
    {
        String another_result = zookeeper->get(getStageNodePath(stage));
        std::lock_guard lock{mutex};
        state.hosts.at(current_host).stages[stage] = another_result;
        return;
    }

    throw zkutil::KeeperException::fromPath(code, getStageNodePath(stage));
}


String BackupCoordinationStageSync::getStageNodePath(const String & stage) const
{
    return fs::path{zookeeper_path} / ("current|" + current_host + "|" + stage);
}


Strings BackupCoordinationStageSync::waitHostsReachStage(const Strings & hosts, const String & stage_to_wait) const
{
    Strings results;
    results.resize(hosts.size());

    std::unique_lock lock{mutex};

    /// TSA_NO_THREAD_SAFETY_ANALYSIS is here because Clang Thread Safety Analysis doesn't understand std::unique_lock.
    auto check_if_hosts_reach_stage = [&]() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return checkIfHostsReachStage(hosts, stage_to_wait, results);
    };

    state_changed.wait(lock, check_if_hosts_reach_stage);

    return results;
}


bool BackupCoordinationStageSync::checkIfHostsReachStage(const Strings & hosts, const String & stage_to_wait, Strings & results) const
{
    process_list_element->checkTimeLimit();
    if (state.host_with_error)
        std::rethrow_exception(state.hosts.at(*state.host_with_error).exception);

    for (size_t i = 0; i != hosts.size(); ++i)
    {
        const String & host = hosts[i];
        auto it = state.hosts.find(host);
        if (it == state.hosts.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "waitHostsReachStage() was called for unexpected {}, all hosts are {}", getHostDesc(host), getHostsDesc(all_hosts));

        const HostInfo & host_info = it->second;
        auto stage_it = host_info.stages.find(stage_to_wait);
        if (stage_it != host_info.stages.end())
        {
            results[i] = stage_it->second;
            continue;
        }

        if (host_info.finished)
        {
            if (stage_to_wait == BackupCoordinationStage::FINALIZING_TABLES)
            {
                /// This is a newly added stage. For compatibility with older server versions,
                /// allow other replicas to skip this stage. This doesn't break anything: this stage
                /// unpauses refreshable materialized views, but older server versions don't pause
                /// them in the first place.
                results[i] = "";
                continue;
            }
            throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                            "{} finished without coming to stage {}", getHostDesc(host), stage_to_wait);
        }

        if (should_stop_watching_thread)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "waitHostsReachStage() can't wait for stage {} after the watching thread stopped", stage_to_wait);

        String host_status;
        if (!host_info.started)
            host_status = fmt::format(": the host hasn't started working on this {} yet", operation_name);
        else if (!host_info.connected)
            host_status = fmt::format(": the host is currently disconnected, last connection was at {}", host_info.last_connection_time);

        LOG_TRACE(log, "Waiting for {} to reach stage {}{}", getHostDesc(host), stage_to_wait, host_status);
        return false; /// wait for next change of `state_changed`
    }

    LOG_INFO(log, "Hosts {} reached stage {}", getHostsDesc(hosts), stage_to_wait);
    return true; /// stop waiting
}


void BackupCoordinationStageSync::finish(bool throw_if_error)
{
    WithRetries::Kind retries_kind = WithRetries::kNormal;
    if (throw_if_error)
        retries_kind = WithRetries::kErrorHandling;

    finishImpl(throw_if_error, retries_kind);
}


void BackupCoordinationStageSync::finishImpl(bool throw_if_error, WithRetries::Kind retries_kind)
{
    {
        std::lock_guard lock{mutex};

        if (finishedNoLock())
        {
            LOG_INFO(log, "The finish node for {} already exists", current_host_desc);
            return;
        }

        if (tried_to_finish[throw_if_error])
        {
            /// We don't repeat creating the finish node, no matter if it was successful or not.
            LOG_INFO(log, "Skipped creating the finish node for {} because earlier we failed to do that", current_host_desc);
            return;
        }

        bool failed_to_set_error = tried_to_set_error && !state.host_with_error;
        if (failed_to_set_error)
        {
            /// Tried to create the 'error' node, but failed.
            /// Then it's better not to create the 'finish' node in this case because otherwise other hosts might think we've succeeded.
            LOG_INFO(log, "Skipping creating the finish node for {} because there was an error which we were unable to send to other hosts", current_host_desc);
            return;
        }

        if (current_host == kInitiator)
        {
            /// Normally the initiator should wait for other hosts to finish before creating its own finish node.
            /// We show warning if some of the other hosts didn't finish.
            bool expect_other_hosts_finished = query_is_sent_to_other_hosts || !state.host_with_error;
            bool other_hosts_finished = otherHostsFinishedNoLock() || !expect_other_hosts_finished;
            if (!other_hosts_finished)
                LOG_WARNING(log, "Hosts {} didn't finish before the initiator", getHostsDesc(getUnfinishedOtherHostsNoLock()));
        }
    }

    SCOPE_EXIT({
        std::lock_guard lock{mutex};
        tried_to_finish[throw_if_error] = true;
    });

    stopWatchingThread();

    auto component_guard = Coordination::setCurrentComponent("BackupCoordinationStageSync::finish");
    try
    {
        auto holder = with_retries.createRetriesControlHolder("BackupCoordinationStageSync::finish", retries_kind);
        holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zookeeper);
            createFinishNodeAndRemoveAliveNode(zookeeper, throw_if_error);
        });
    }
    catch (...)
    {
        LOG_TRACE(log, "Caught exception while creating the 'finish' node for {}: {}",
            current_host_desc,
            getCurrentExceptionMessage(/* with_stacktrace= */ false, /* check_embedded_stacktrace= */ true));

        if (throw_if_error)
            throw;
    }
}


void BackupCoordinationStageSync::createFinishNodeAndRemoveAliveNode(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper, bool throw_if_error)
{
    std::optional<size_t> num_hosts;
    int num_hosts_version = -1;

    auto unfinished_hosts = getUnfinishedHosts();
    std::vector<std::pair<size_t, String>> non_existent_node_pos;
    non_existent_node_pos.reserve(unfinished_hosts.size());

    for (size_t attempt_no = 1; attempt_no <= max_attempts_after_bad_version; ++attempt_no)
    {
        if (throw_if_error)
        {
            /// Rethrow the current error if there is an error node.
            if (isErrorSet())
                rethrowSetError();

            String serialized_error;
            if (zookeeper->tryGet(error_node_path, serialized_error))
            {
                auto [exception, host] = parseErrorNode(serialized_error);
                if (exception)
                {
                    std::lock_guard lock{mutex};
                    state.addErrorInfo(exception, host);
                    std::rethrow_exception(exception);
                }
            }
        }

        /// The 'num_hosts' node may not exist if createStartAndAliveNodes() failed when startup() was called.
        if (!num_hosts)
        {
            String num_hosts_str;
            Coordination::Stat stat;
            if (zookeeper->tryGet(num_hosts_node_path, num_hosts_str, &stat))
            {
                num_hosts = parseFromString<size_t>(num_hosts_str);
                num_hosts_version = stat.version;
            }
        }

        /// Read the "finish" nodes related to the current host and other hosts and update the current state.
        /// We need to that here because after calling function finish() we usually call function allHostsFinished().
        if (std::erase_if(unfinished_hosts, [&](const String & host) { return zookeeper->exists(zookeeper_path / ("finished|" + host)); }))
        {
            std::lock_guard lock{mutex};
            for (auto & [host, host_info] : state.hosts)
            {
                if (!host_info.finished && (std::find(unfinished_hosts.begin(), unfinished_hosts.end(), host) == unfinished_hosts.end()))
                    host_info.finished = true;
            }
        }

        if (finished())
        {
            /// The "finish" node for the current host already exists.
            /// That can happen if previous attempt failed because of a connection loss but was in fact successful.
            LOG_INFO(log, "The 'finish' node in ZooKeeper for {} already exists. Hosts {} haven't finished yet",
                     current_host_desc, getHostsDesc(getUnfinishedOtherHosts()));
            return;
        }

        bool start_node_exists = zookeeper->exists(start_node_path);

        Coordination::Requests requests;
        requests.reserve(3 + unfinished_hosts.size());

        requests.emplace_back(zkutil::makeCreateRequest(finish_node_path, "", zkutil::CreateMode::Persistent));

        size_t alive_node_pos = static_cast<size_t>(-1);
        if (zookeeper->exists(alive_node_path))
        {
            alive_node_pos = requests.size();
            requests.emplace_back(zkutil::makeRemoveRequest(alive_node_path, -1));
        }

        size_t num_hosts_node_pos = static_cast<size_t>(-1);
        if (num_hosts)
        {
            num_hosts_node_pos = requests.size();
            requests.emplace_back(zkutil::makeSetRequest(num_hosts_node_path, toString(start_node_exists ? (*num_hosts - 1) : *num_hosts), num_hosts_version));
        }

        non_existent_node_pos.clear();
        for (const String & host : unfinished_hosts)
        {
            if (host != current_host)
            {
                String node_path = zookeeper_path / ("finished|" + host);
                non_existent_node_pos.emplace_back(requests.size(), node_path);
                zkutil::addCheckNotExistsRequest(requests, *zookeeper, node_path);
            }
        }

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(requests, responses);

        if (code == Coordination::Error::ZOK)
        {
            LOG_INFO(log, "Created the 'finish' node in ZooKeeper for {}. Hosts {} haven't finished yet",
                     current_host_desc, getHostsDesc(getUnfinishedOtherHosts()));
            std::lock_guard lock{mutex};
            state.hosts.at(current_host).finished = true;
            return;
        }

        auto get_node_failed_check_for_non_existence = [&]() -> String
        {
            for (const auto & [pos, node_path] : non_existent_node_pos)
            {
                if ((pos < responses.size()) && (responses[pos]->error == Coordination::Error::ZNODEEXISTS))
                    return node_path;
            }
            return "";
        };

        auto show_error_before_next_attempt = [&](const String & message)
        {
            bool will_try_again = (attempt_no < max_attempts_after_bad_version);
            LOG_TRACE(log, "{} (attempt #{}){}", message, attempt_no, will_try_again ? ", will try again" : "");
            if (!will_try_again)
            {
                throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                                "Couldn't create the 'finish' node for {} after {} attempts: {}", current_host_desc, attempt_no, message);
            }
        };

        if ((alive_node_pos < responses.size()) &&
            (responses[alive_node_pos]->error == Coordination::Error::ZNONODE))
        {
            show_error_before_next_attempt(fmt::format("Node {} doesn't exist", alive_node_path));
            /// needs another attempt
        }
        else if ((num_hosts_node_pos < responses.size()) &&
                 (responses[num_hosts_node_pos]->error == Coordination::Error::ZBADVERSION))
        {
            show_error_before_next_attempt(fmt::format("The version of node {} changed", num_hosts_node_path));
            num_hosts.reset(); /// needs to reread 'num_hosts' again
        }
        else if (!get_node_failed_check_for_non_existence().empty())
        {
            show_error_before_next_attempt(fmt::format("Node {} exists", get_node_failed_check_for_non_existence()));
            /// needs another attempt
        }
        else
        {
            zkutil::KeeperMultiException::check(code, requests, responses);
        }
    }

    UNREACHABLE();
}


int BackupCoordinationStageSync::getInitiatorVersion() const
{
    std::lock_guard lock{mutex};
    return state.hosts.at(String{kInitiator}).version;
}


void BackupCoordinationStageSync::waitOtherHostsFinish(bool throw_if_error) const
{
    std::optional<std::chrono::seconds> timeout;
    String reason;

    if (!throw_if_error)
    {
        if (finish_timeout_after_error.count() != 0)
            timeout = finish_timeout_after_error;
        reason = "after error before cleanup";
    }

    waitOtherHostsFinishImpl(reason, timeout, throw_if_error);
}


void BackupCoordinationStageSync::waitOtherHostsFinishImpl(const String & reason, std::optional<std::chrono::seconds> timeout, bool throw_if_error) const
{
    UniqueLock lock{mutex};

    if (otherHostsFinishedNoLock())
    {
        LOG_TRACE(log, "Other hosts have already finished");
        return;
    }

    bool failed_to_set_error = tried_to_set_error && !state.host_with_error;
    if (failed_to_set_error)
    {
        /// Tried to create the 'error' node, but failed.
        /// Then it's better not to wait for other hosts to finish in this case because other hosts don't know they should finish.
        LOG_INFO(log, "Skipping waiting for other hosts to finish because there was an error which we were unable to send to other hosts");
        return;
    }

    if (timeout)
    {
        if (!state_changed.wait_for(
                lock.getUnderlyingLock(),
                *timeout,
                [&] TSA_REQUIRES(mutex) { return checkIfOtherHostsFinish(reason, timeout, false, throw_if_error); }))
            checkIfOtherHostsFinish(reason, timeout, true, throw_if_error);
    }
    else
    {
        state_changed.wait(
            lock.getUnderlyingLock(),
            [&] TSA_REQUIRES(mutex) { return checkIfOtherHostsFinish(reason, timeout, false, throw_if_error); });
    }
}


bool BackupCoordinationStageSync::checkIfOtherHostsFinish(
    const String & reason, std::optional<std::chrono::milliseconds> timeout, bool time_is_out, bool throw_if_error) const
{
    if (throw_if_error)
    {
        process_list_element->checkTimeLimit();
        if (state.host_with_error)
            std::rethrow_exception(state.hosts.at(*state.host_with_error).exception);
    }

    for (const auto & [host, host_info] : state.hosts)
    {
        if ((host == current_host) || host_info.finished)
            continue;

        String reason_text = reason.empty() ? "" : (" " + reason);

        String host_status;
        if (!host_info.started)
            host_status = fmt::format(": the host hasn't started working on this {} yet", operation_name);
        else if (!host_info.connected)
            host_status = fmt::format(": the host is currently disconnected, last connection was at {}", host_info.last_connection_time);

        if (time_is_out)
        {
            if (throw_if_error)
            {
                throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                                "Waited longer than timeout {} for {} to finish{}{}",
                                *timeout, getHostDesc(host), reason_text, host_status);
            }
            LOG_INFO(log, "Waited longer than timeout {} for {} to finish{}{}",
                     *timeout, getHostDesc(host), reason_text, host_status);
            return true; /// stop waiting
        }

        if (should_stop_watching_thread)
        {
            LOG_ERROR(log, "waitOtherHostFinish({}) can't wait for other hosts to finish after the watching thread stopped", throw_if_error);
            chassert(false, "waitOtherHostFinish() can't wait for other hosts to finish after the watching thread stopped");
            if (throw_if_error)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "waitOtherHostsFinish() can't wait for other hosts to finish after the watching thread stopped");
            return true; /// stop waiting
        }

        LOG_TRACE(log, "Waiting for {} to finish{}{}", getHostDesc(host), reason_text, host_status);
        return false; /// wait for next change of `state_changed`
    }

    LOG_TRACE(log, "Other hosts finished working on this {}", operation_name);
    return true; /// stop waiting
}


bool BackupCoordinationStageSync::finished() const
{
    std::lock_guard lock{mutex};
    return finishedNoLock();
}


bool BackupCoordinationStageSync::finishedNoLock() const
{
    return state.hosts.at(current_host).finished;
}


bool BackupCoordinationStageSync::otherHostsFinished() const
{
    std::lock_guard lock{mutex};
    return otherHostsFinishedNoLock();
}


bool BackupCoordinationStageSync::otherHostsFinishedNoLock() const
{
    for (const auto & [host, host_info] : state.hosts)
    {
        if (!host_info.finished && (host != current_host))
            return false;
    }
    return true;
}


bool BackupCoordinationStageSync::allHostsFinishedNoLock() const
{
    return finishedNoLock() && otherHostsFinishedNoLock();
}


Strings BackupCoordinationStageSync::getUnfinishedHosts() const
{
    std::lock_guard lock{mutex};
    return getUnfinishedHostsNoLock();
}


Strings BackupCoordinationStageSync::getUnfinishedHostsNoLock() const
{
    if (allHostsFinishedNoLock())
        return {};

    Strings res;
    res.reserve(all_hosts.size());
    for (const auto & [host, host_info] : state.hosts)
    {
        if (!host_info.finished)
            res.emplace_back(host);
    }
    return res;
}


Strings BackupCoordinationStageSync::getUnfinishedOtherHosts() const
{
    std::lock_guard lock{mutex};
    return getUnfinishedOtherHostsNoLock();
}


Strings BackupCoordinationStageSync::getUnfinishedOtherHostsNoLock() const
{
    if (otherHostsFinishedNoLock())
        return {};

    Strings res;
    res.reserve(all_hosts.size() - 1);
    for (const auto & [host, host_info] : state.hosts)
    {
        if (!host_info.finished && (host != current_host))
            res.emplace_back(host);
    }
    return res;
}


void BackupCoordinationStageSync::setError(std::exception_ptr exception, bool throw_if_error)
{
    try
    {
        std::rethrow_exception(exception);
    }
    catch (const Exception & e)
    {
        setError(e, throw_if_error);
    }
    catch (...)
    {
        setError(Exception{getCurrentExceptionMessageAndPattern(true, true), getCurrentExceptionCode()}, throw_if_error);
    }
}


void BackupCoordinationStageSync::setError(const Exception & exception, bool throw_if_error)
{
    /// Most likely this exception has been already logged so here we're logging it without stacktrace.
    String exception_message = getExceptionMessage(exception, /* with_stacktrace= */ false, /* check_embedded_stacktrace= */ true);
    LOG_INFO(log, "Sending exception from {} to other hosts: {}", current_host_desc, exception_message);

    {
        std::lock_guard lock{mutex};
        if (state.host_with_error)
        {
            /// We create the error node always before assigning `state.host_with_error`,
            /// thus if `state.host_with_error` is set then we can be sure that the error node exists.
            LOG_INFO(log, "The error node already exists");
            return;
        }

        if (tried_to_set_error)
        {
            LOG_INFO(log, "Skipped creating the error node because earlier we failed to do that");
            return;
        }
    }

    SCOPE_EXIT({
        std::lock_guard lock{mutex};
        tried_to_set_error = true;
    });

    try
    {


        auto holder = with_retries.createRetriesControlHolder("BackupCoordinationStageSync::setError", WithRetries::kErrorHandling);
        holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zookeeper);
            createErrorNode(exception, zookeeper);
        });
    }
    catch (...)
    {
        LOG_TRACE(log, "Caught exception while creating the error node for this {}: {}",
                  is_restore ? "restore" : "backup",
                  getCurrentExceptionMessage(/* with_stacktrace= */ false, /* check_embedded_stacktrace= */ true));

        if (throw_if_error)
            throw;
    }
}


void BackupCoordinationStageSync::createErrorNode(const Exception & exception, Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper)
{
    if (isErrorSet())
    {
        /// We create the error node always before assigning `state.host_with_error`,
        /// thus if `state.host_with_error` is set then we can be sure that the error node exists.
        LOG_INFO(log, "The error node already exists");
        return;
    }

    String serialized_error;
    {
        WriteBufferFromOwnString buf;
        writeStringBinary(current_host, buf);
        writeException(exception, buf, true);
        serialized_error = buf.str();
    }

    zookeeper->createIfNotExists(operation_zookeeper_path, "");
    zookeeper->createIfNotExists(zookeeper_path, "");

    auto code = zookeeper->tryCreate(error_node_path, serialized_error, zkutil::CreateMode::Persistent);

    if (code == Coordination::Error::ZOK)
    {
        std::lock_guard lock{mutex};
        state.addErrorInfo(parseErrorNode(serialized_error).first, current_host);
        LOG_TRACE(log, "Sent exception from {} to other hosts", current_host_desc);
        return;
    }

    if (code == Coordination::Error::ZNODEEXISTS)
    {
        String another_error = zookeeper->get(error_node_path);
        auto [another_exception, host] = parseErrorNode(another_error);
        if (another_exception)
        {
            std::lock_guard lock{mutex};
            state.addErrorInfo(another_exception, host);
            LOG_INFO(log, "Another error is already assigned for this {}", operation_name);
            return;
        }
    }

    throw zkutil::KeeperException::fromPath(code, error_node_path);
}


std::pair<std::exception_ptr, String> BackupCoordinationStageSync::parseErrorNode(const String & error_node_contents) const
{
    ReadBufferFromOwnString buf{error_node_contents};
    String host;
    readStringBinary(host, buf);
    if (std::find(all_hosts.begin(), all_hosts.end(), host) == all_hosts.end())
        return {};
    auto exception = std::make_exception_ptr(readException(buf, fmt::format("Got error from {}", getHostDesc(host))));
    return {exception, host};
}


bool BackupCoordinationStageSync::isErrorSet() const
{
    std::lock_guard lock{mutex};
    return state.host_with_error.has_value();
}

void BackupCoordinationStageSync::rethrowSetError() const
{
    std::lock_guard lock{mutex};
    chassert(state.host_with_error);
    std::rethrow_exception(state.hosts.at(*state.host_with_error).exception);
}

}
