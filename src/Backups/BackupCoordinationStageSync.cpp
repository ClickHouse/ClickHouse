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
#include <Backups/BackupLocalConcurrencyChecker.h>
#include <Poco/URI.h>
#include <boost/algorithm/string/join.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int FAILED_TO_SYNC_BACKUP_OR_RESTORE;
    extern const int LOGICAL_ERROR;
}

bool BackupCoordinationStageSync::HostInfo::operator ==(const HostInfo & other) const
{
    /// We don't compare `last_connection_time` here.
    return (host == other.host) && (started == other.started) && (connected == other.connected) && (finished == other.finished)
        && (stages == other.stages) && (!!exception == !!other.exception) && (disconnected_too_long == other.disconnected_too_long);
}

bool BackupCoordinationStageSync::HostInfo::operator !=(const HostInfo & other) const
{
    return !(*this == other);
}

bool BackupCoordinationStageSync::State::operator ==(const State & other) const = default;
bool BackupCoordinationStageSync::State::operator !=(const State & other) const = default;


BackupCoordinationStageSync::BackupCoordinationStageSync(
        bool is_restore_,
        const String & zookeeper_path_,
        const String & current_host_,
        bool allow_concurrency_,
        const WithRetries & with_retries_,
        ThreadPoolCallbackRunnerUnsafe<void> schedule_,
        QueryStatusPtr process_list_element_,
        LoggerPtr log_)
    : is_restore(is_restore_)
    , operation_name(is_restore ? "restore" : "backup")
    , current_host(current_host_)
    , current_host_desc(getHostDesc(current_host))
    , allow_concurrency(allow_concurrency_)
    , with_retries(with_retries_)
    , schedule(schedule_)
    , process_list_element(process_list_element_)
    , log(log_)
    , failure_after_host_disconnected_for_seconds(with_retries.getKeeperSettings().failure_after_host_disconnected_for_seconds)
    , error_handling_timeout(with_retries.getKeeperSettings().on_cluster_error_handling_timeout)
    , sync_period_ms(with_retries.getKeeperSettings().sync_period_ms)
    , max_attempts_after_bad_version(with_retries.getKeeperSettings().max_attempts_after_bad_version)
    , zookeeper_path(zookeeper_path_)
    , root_zookeeper_path(zookeeper_path.parent_path().parent_path())
    , operation_node_path(zookeeper_path.parent_path())
    , operation_node_name(zookeeper_path.parent_path().filename())
    , stage_node_path(zookeeper_path)
    , start_node_path(zookeeper_path / ("started|" + current_host))
    , finish_node_path(zookeeper_path / ("finished|" + current_host))
    , num_hosts_node_path(zookeeper_path / "num_hosts")
    , alive_node_path(zookeeper_path / ("alive|" + current_host))
    , alive_tracker_node_path(fs::path{root_zookeeper_path} / "alive_tracker")
    , error_node_path(zookeeper_path / "error")
    , zk_nodes_changed(std::make_shared<Poco::Event>())
{
    if ((zookeeper_path.filename() != "stage") || !operation_node_name.starts_with(is_restore ? "restore-" : "backup-")
        || (root_zookeeper_path == operation_node_path))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected path in ZooKeeper specified: {}", zookeeper_path);
    }

    createRootNodes();
    createStartAndAliveNodes();

    bool ok = false;
    SCOPE_EXIT({
        if (!ok)
        {
            /// startWatchingThread() below can fail, in that case we need to
            /// remove the nodes we created in createStartAndAliveNodes().
            tryRemoveStartAndAliveNodes();
        }
    });

    startWatchingThread();
    ok = true;
}


BackupCoordinationStageSync::~BackupCoordinationStageSync()
{
    /// tryFinish() must not throw any exceptions.
    tryFinish();
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


void BackupCoordinationStageSync::createRootNodes()
{
    auto holder = with_retries.createRetriesControlHolder("BackupStageSync::createRootNodes", {.initialization = true});
    holder.retries_ctl.retryLoop(
        [&, &zookeeper = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zookeeper);
            zookeeper->createAncestors(root_zookeeper_path);
            zookeeper->createIfNotExists(root_zookeeper_path, "");
        });
}


void BackupCoordinationStageSync::createStartAndAliveNodes()
{
    auto holder = with_retries.createRetriesControlHolder("BackupStageSync::createStartAndAliveNodes", {.initialization = true});
    holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zookeeper);
        createStartAndAliveNodes(zookeeper);
    });
}


void BackupCoordinationStageSync::createStartAndAliveNodes(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper)
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

        Coordination::Requests requests;

        size_t operation_node_path_pos = static_cast<size_t>(-1);
        if (!zookeeper->exists(operation_node_path))
        {
            operation_node_path_pos = requests.size();
            requests.emplace_back(zkutil::makeCreateRequest(operation_node_path, "", zkutil::CreateMode::Persistent));
        }

        size_t stage_node_path_pos = static_cast<size_t>(-1);
        if (!zookeeper->exists(stage_node_path))
        {
            stage_node_path_pos = requests.size();
            requests.emplace_back(zkutil::makeCreateRequest(stage_node_path, "", zkutil::CreateMode::Persistent));
        }

        size_t num_hosts_node_path_pos = requests.size();
        if (num_hosts)
            requests.emplace_back(zkutil::makeSetRequest(num_hosts_node_path, toString(*num_hosts + 1), num_hosts_version));
        else
            requests.emplace_back(zkutil::makeCreateRequest(num_hosts_node_path, "1", zkutil::CreateMode::Persistent));

        size_t alive_tracker_node_path_pos = requests.size();
        requests.emplace_back(zkutil::makeSetRequest(alive_tracker_node_path, "", alive_tracker_version));

        requests.emplace_back(zkutil::makeCreateRequest(start_node_path, "", zkutil::CreateMode::Persistent));
        requests.emplace_back(zkutil::makeCreateRequest(alive_node_path, "", zkutil::CreateMode::Ephemeral));

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(requests, responses);

        if (code == Coordination::Error::ZOK)
        {
            LOG_INFO(log, "Created start node #{} in ZooKeeper for {}", num_hosts.value_or(0) + 1, current_host_desc);
            return;
        }

        auto show_error_before_next_attempt = [&](const String & message)
        {
            bool will_try_again = (attempt_no < max_attempts_after_bad_version);
            LOG_TRACE(log, "{} (attempt #{}){}", message, attempt_no, will_try_again ? ", will try again" : "");
        };

        if ((responses.size() > operation_node_path_pos) &&
            (responses[operation_node_path_pos]->error == Coordination::Error::ZNODEEXISTS))
        {
            show_error_before_next_attempt(fmt::format("Node {} in ZooKeeper already exists", operation_node_path));
            /// need another attempt
        }
        else if ((responses.size() > stage_node_path_pos) &&
            (responses[stage_node_path_pos]->error == Coordination::Error::ZNODEEXISTS))
        {
            show_error_before_next_attempt(fmt::format("Node {} in ZooKeeper already exists", stage_node_path));
            /// need another attempt
        }
        else if ((responses.size() > num_hosts_node_path_pos) && num_hosts &&
            (responses[num_hosts_node_path_pos]->error == Coordination::Error::ZBADVERSION))
        {
            show_error_before_next_attempt("Other host changed the 'num_hosts' node in ZooKeeper");
            num_hosts.reset(); /// need to reread 'num_hosts' again
        }
        else if ((responses.size() > num_hosts_node_path_pos) && num_hosts &&
            (responses[num_hosts_node_path_pos]->error == Coordination::Error::ZNONODE))
        {
            show_error_before_next_attempt("Other host removed the 'num_hosts' node in ZooKeeper");
            num_hosts.reset(); /// need to reread 'num_hosts' again
        }
        else if ((responses.size() > num_hosts_node_path_pos) && !num_hosts &&
            (responses[num_hosts_node_path_pos]->error == Coordination::Error::ZNODEEXISTS))
        {
            show_error_before_next_attempt("Other host created the 'num_hosts' node in ZooKeeper");
            /// need another attempt
        }
        else if ((responses.size() > alive_tracker_node_path_pos) &&
            (responses[alive_tracker_node_path_pos]->error == Coordination::Error::ZBADVERSION))
        {
            show_error_before_next_attempt("Concurrent backup or restore changed some 'alive' nodes in ZooKeeper");
            check_concurrency = true; /// need to recheck for concurrency again
        }
        else
        {
            zkutil::KeeperMultiException::check(code, requests, responses);
        }
    }

    throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                    "Couldn't create the 'start' node in ZooKeeper for {} after {} attempts",
                    current_host_desc, max_attempts_after_bad_version);
}


bool BackupCoordinationStageSync::tryRemoveStartAndAliveNodes()
{
    try
    {
        auto holder = with_retries.createRetriesControlHolder("BackupStageSync::removeStartAndAliveNodes", {.error_handling = true});
        holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zookeeper);
            removeStartNode(zookeeper);
            removeAliveNode(zookeeper);
        });
        return true;
    }
    catch (...)
    {
        /// retryLoop() inside removeStartNode() and removeAliveNode() has already logged this exception.
        return false;
    }
}


void BackupCoordinationStageSync::removeStartNode(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper)
{
    if (!zookeeper->exists(start_node_path))
        return;

    std::optional<size_t> num_hosts;
    int num_hosts_version = -1;

    for (size_t attempt_no = 1; attempt_no <= max_attempts_after_bad_version; ++attempt_no)
    {
        if (!num_hosts)
        {
            Coordination::Stat stat;
            num_hosts = parseFromString<size_t>(zookeeper->get(num_hosts_node_path, &stat));
            num_hosts_version = stat.version;
        }

        Coordination::Requests requests;
        requests.emplace_back(zkutil::makeRemoveRequest(start_node_path, -1));
        requests.emplace_back(zkutil::makeSetRequest(num_hosts_node_path, toString(*num_hosts - 1), num_hosts_version));

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(requests, responses);

        if (!((code == Coordination::Error::ZOK) || (code == Coordination::Error::ZBADVERSION)))
        {
            /// callAndCatchAll() will catch that and log.
            zkutil::KeeperMultiException::check(code, requests, responses);
        }

        if (code == Coordination::Error::ZOK)
        {
            LOG_TRACE(log, "Removed the 'start' node for {}", current_host_desc);
            return;
        }

        chassert(code == Coordination::Error::ZBADVERSION);
        bool will_try_again = (attempt_no < max_attempts_after_bad_version);

        LOG_TRACE(log, "Other host changed the 'num_hosts' node in ZooKeeper (attempt #{}){}", attempt_no,
                  (will_try_again ? ", will try again" : ""));
        num_hosts.reset(); /// need to reread 'num_hosts' again
    }

    throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                    "Couldn't remove the 'start' node from ZooKeeper for {} after {} attempts",
                    current_host_desc, max_attempts_after_bad_version);
}


void BackupCoordinationStageSync::removeAliveNode(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper)
{
    if (!zookeeper->exists(alive_node_path))
        return;

    auto code = zookeeper->tryRemove(alive_node_path);
    if (!((code == Coordination::Error::ZOK) || (code == Coordination::Error::ZNONODE)))
        throw zkutil::KeeperException::fromPath(code, alive_node_path);

    if (code == Coordination::Error::ZOK)
        LOG_TRACE(log, "Removed the 'alive' node for {}", current_host_desc);
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
                        BackupLocalConcurrencyChecker::throwConcurrentOperationNotAllowed(is_restore);
                }
            }
        }
    }
}


void BackupCoordinationStageSync::startWatchingThread()
{
    watching_thread_future = schedule([this]() { watchingThread(); }, Priority{});
}


void BackupCoordinationStageSync::stopWatchingThread()
{
    should_stop_watching_thread = true;

    /// Wake up waiting threads.
    if (zk_nodes_changed)
        zk_nodes_changed->set();
    state_changed.notify_all();

    if (watching_thread_future.valid())
        watching_thread_future.wait();
}


void BackupCoordinationStageSync::watchingThread()
{
    auto holder = with_retries.createRetriesControlHolder("BackupStageSync::watchingThread");

    while (!should_stop_watching_thread)
    {
        try
        {
            /// Check if the current BACKUP or RESTORE command is already cancelled.
            checkIfQueryCancelled();

            /// Recreate the 'alive' node if necessary and read a new state from ZooKeeper.
            auto & zookeeper = holder.faulty_zookeeper;
            with_retries.renewZooKeeper(zookeeper);

            if (should_stop_watching_thread)
                return;

            /// Recreate the 'alive' node if it was removed.
            createAliveNode(zookeeper);

            /// Reads the current state from nodes in ZooKeeper.
            readCurrentState(zookeeper);

            /// Check if the current state contains error.
            cancelQueryIfError();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caugth exception while watching");
        }

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


void BackupCoordinationStageSync::readCurrentState(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper)
{
    zk_nodes_changed->reset();

    /// Get zk nodes and subscribe on their changes.
    Strings new_zk_nodes = zookeeper->getChildren(stage_node_path, nullptr, zk_nodes_changed);
    std::sort(new_zk_nodes.begin(), new_zk_nodes.end()); /// Sorting is necessary because we compare the list of zk nodes with its previous versions.

    State new_state;

    {
        std::lock_guard lock{mutex};

        Strings added_zk_nodes, removed_zk_nodes;
        std::set_difference(new_zk_nodes.begin(), new_zk_nodes.end(), zk_nodes.begin(), zk_nodes.end(), back_inserter(added_zk_nodes));
        std::set_difference(zk_nodes.begin(), zk_nodes.end(), new_zk_nodes.begin(), new_zk_nodes.end(), back_inserter(removed_zk_nodes));
        if (!added_zk_nodes.empty())
            LOG_TRACE(log, "Detected new zookeeper nodes appeared in the stage folder: {}", boost::algorithm::join(added_zk_nodes, ", "));
        if (!removed_zk_nodes.empty())
            LOG_TRACE(log, "Detected that some zookeeper nodes disappeared from the stage folder: {}", boost::algorithm::join(removed_zk_nodes, ", "));

        zk_nodes = new_zk_nodes;
        new_state = state;
    }

    auto get_host_info = [&](const String & host) -> HostInfo &
    {
        auto it = new_state.hosts.find(host);
        if (it == new_state.hosts.end())
            it = new_state.hosts.emplace(host, HostInfo{.host = host}).first;
        return it->second;
    };

    auto now = std::chrono::system_clock::now();
    auto monotonic_now = std::chrono::steady_clock::now();

    /// Read the current state of zk nodes.
    for (const auto & zk_node : new_zk_nodes)
    {
        if (zk_node == "error")
        {
            if (!new_state.host_with_error)
            {
                String serialized_error = zookeeper->get(fs::path{stage_node_path} / "error");
                ReadBufferFromOwnString buf{serialized_error};
                String host;
                readStringBinary(host, buf);
                auto & host_info = get_host_info(host);
                host_info.exception = std::make_exception_ptr(readException(buf, fmt::format("Got error from {}", getHostDesc(host))));
                new_state.host_with_error = host;
            }
        }
        else if (zk_node.starts_with("started|"))
        {
            String host = zk_node.substr(strlen("started|"));
            auto & host_info = get_host_info(host);
            if (!host_info.started)
            {
                host_info.started = true;
                host_info.connected = true;
                host_info.last_connection_time = now;
                host_info.last_connection_time_monotonic = monotonic_now;
            }
        }
        else if (zk_node.starts_with("finished|"))
        {
            String host = zk_node.substr(strlen("finished|"));
            auto & host_info = get_host_info(host);
            host_info.finished = true;
        }
        else if (zk_node.starts_with("alive|"))
        {
            String host = zk_node.substr(strlen("alive|"));
            auto & host_info = get_host_info(host);
            host_info.connected = true;
            host_info.last_connection_time = now;
            host_info.last_connection_time_monotonic = monotonic_now;
            host_info.disconnected_too_long = false;
        }
        else if (zk_node.starts_with("current|"))
        {
            String host_and_stage = zk_node.substr(strlen("current|"));
            size_t separator_pos = host_and_stage.find('|');
            if (separator_pos != String::npos)
            {
                String host = host_and_stage.substr(0, separator_pos);
                String stage = host_and_stage.substr(separator_pos + 1);
                auto & host_info = get_host_info(host);
                String result = zookeeper->get(fs::path{zookeeper_path} / zk_node);
                host_info.stages[stage] = std::move(result);
            }
        }
    }

    bool was_state_changed = false;
    {
        std::lock_guard lock{mutex};

        if (failure_after_host_disconnected_for_seconds.count() > 0)
        {
            for (auto & [host, host_info] : new_state.hosts)
            {
                if (!host_info.connected && host_info.started && !host_info.finished && !host_info.disconnected_too_long
                    && (monotonic_now - host_info.last_connection_time_monotonic >= failure_after_host_disconnected_for_seconds))
                {
                    host_info.disconnected_too_long = true;
                    LOG_WARNING(log, "Host {} has not been updating its 'alive' node for more than {}",
                                getHostDesc(host), failure_after_host_disconnected_for_seconds);
                }
            }
        }

        was_state_changed = (new_state != state);
        state = std::move(new_state);
    }

    if (was_state_changed)
        state_changed.notify_all();
}


void BackupCoordinationStageSync::checkIfQueryCancelled()
{
    if (process_list_element->checkTimeLimitSoft())
        return; /// Not cancelled.

    std::lock_guard lock{mutex};
    if (state.cancelled)
        return; /// Already marked as cancelled.

    state.cancelled = true;
    state_changed.notify_all();
}


void BackupCoordinationStageSync::cancelQueryIfError()
{
    std::exception_ptr exception;

    {
        std::lock_guard lock{mutex};
        if (state.cancelled || !state.host_with_error)
            return;

        state.cancelled = true;
        exception = state.hosts.at(*state.host_with_error).exception;
    }

    process_list_element->cancelQuery(false, exception);
    state_changed.notify_all();
}


void BackupCoordinationStageSync::setStage(const String & stage, const String & stage_result)
{
    LOG_INFO(log, "{} reached stage {}", current_host_desc, stage);
    auto holder = with_retries.createRetriesControlHolder("BackupStageSync::setStage");
    holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zookeeper);
        zookeeper->createIfNotExists(getStageNodePath(stage), stage_result);
    });
}


String BackupCoordinationStageSync::getStageNodePath(const String & stage) const
{
    return fs::path{zookeeper_path} / ("current|" + current_host + "|" + stage);
}


void BackupCoordinationStageSync::setError(const Exception & exception)
{
    /// Most likely this exception has been already logged so here we're logging it without stacktrace.
    String exception_message = getExceptionMessage(exception, /* with_stacktrace= */ false, /* check_embedded_stacktrace= */ true);
    LOG_INFO(log, "Sending exception from {} to other hosts: {}", current_host_desc, exception_message);

    auto holder = with_retries.createRetriesControlHolder("BackupStageSync::setError");
    holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zookeeper);

        WriteBufferFromOwnString buf;
        writeStringBinary(current_host, buf);
        writeException(exception, buf, true);
        auto code = zookeeper->tryCreate(error_node_path, buf.str(), zkutil::CreateMode::Persistent);

        if (!((code == Coordination::Error::ZOK) || (code == Coordination::Error::ZNODEEXISTS)))
            throw zkutil::KeeperException::fromPath(code, error_node_path);

        if (code == Coordination::Error::ZOK)
            LOG_TRACE(log, "Sent exception from {} to other hosts", current_host_desc);
        else
            LOG_INFO(log, "There is already an error assigned to this {}", operation_name);
    });
}


Strings BackupCoordinationStageSync::waitHostsReachStage(const String & stage_to_wait, const Strings & hosts, std::optional<std::chrono::milliseconds> timeout) const
{
    Strings results;
    results.resize(hosts.size());

    std::unique_lock lock{mutex};

    /// TSA_NO_THREAD_SAFETY_ANALYSIS is here because Clang Thread Safety Analysis doesn't understand std::unique_lock.
    auto check_if_hosts_ready = [&](bool throw_if_not_ready) TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return checkIfHostsReachStage(hosts, stage_to_wait, timeout, throw_if_not_ready, results);
    };

    if (timeout)
    {
        if (!state_changed.wait_for(lock, *timeout, [&] { return check_if_hosts_ready(/* throw_if_not_ready= */ false); }))
            check_if_hosts_ready(/* throw_if_not_ready= */ true);
    }
    else
    {
        state_changed.wait(lock, [&] { return check_if_hosts_ready(/* throw_if_not_ready= */ false); });
    }

    return results;
}


bool BackupCoordinationStageSync::checkIfHostsReachStage(
    const Strings & hosts,
    const String & stage_to_wait,
    std::optional<std::chrono::milliseconds> timeout,
    bool throw_if_not_ready,
    Strings & results) const
{
    if (should_stop_watching_thread)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "finish() was called while another thread is waiting for a stage");

    if (state.host_with_error)
        std::rethrow_exception(state.hosts.at(*state.host_with_error).exception);

    process_list_element->checkTimeLimit();

    for (size_t i = 0; i != hosts.size(); ++i)
    {
        const String & host = hosts[i];
        auto it = state.hosts.find(host);

        bool never_connected = (it == state.hosts.end()) || !it->second.started;
        if (never_connected)
        {
            if (throw_if_not_ready)
            {
                throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Never connected to {}", getHostDesc(host));
            }
            else
            {
                LOG_TRACE(log, "Waiting for {} to connect", getHostDesc(host));
                return false;
            }
        }

        const HostInfo & host_info = it->second;
        auto stage_it = host_info.stages.find(stage_to_wait);
        if (stage_it != host_info.stages.end())
        {
            results[i] = stage_it->second;
            continue;
        }

        if (host_info.finished)
        {
            throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "{} finished without coming to stage {}",
                            getHostDesc(host), stage_to_wait);
        }
        else if (host_info.connected)
        {
            if (throw_if_not_ready)
            {
                String timeout_desc = timeout ? fmt::format(" ({})", *timeout) : "";
                throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Waited for {} to come to stage {} for too long{}",
                                getHostDesc(host), stage_to_wait, timeout_desc);
            }
            else
            {
                LOG_TRACE(log, "Waiting for {} to come to stage {}", getHostDesc(host), stage_to_wait);
                return false;
            }
        }
        else
        {
            chassert(!host_info.connected && host_info.started && !host_info.finished);
            auto disconnected_seconds = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - host_info.last_connection_time_monotonic);
            String last_connection_desc = fmt::format(", last time the host was connected at {}", host_info.last_connection_time);
            if (throw_if_not_ready)
            {
                String timeout_desc = timeout ? fmt::format(" ({})", *timeout) : "";
                throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                                "Lost connection to {} for {} while waiting for the host to come to stage {} for too long{}{}",
                                getHostDesc(host), disconnected_seconds, stage_to_wait, timeout_desc, last_connection_desc);
            }
            else if (host_info.disconnected_too_long)
            {
                throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                                "Lost connection to {} for {} which is more than the threshold for the failure set to {}{}",
                                getHostDesc(host), disconnected_seconds, failure_after_host_disconnected_for_seconds, last_connection_desc);
            }
            else
            {
                LOG_TRACE(log, "Waiting for reconnection to {}{}", getHostDesc(host), last_connection_desc);
                return false;
            }
        }
    }

    LOG_INFO(log, "All hosts reached stage {}", stage_to_wait);
    return true;
}


void BackupCoordinationStageSync::finish()
{
    if (finished)
        return;

    chassert(!failed_to_finish);
    try
    {
        stopWatchingThread();

        auto holder = with_retries.createRetriesControlHolder("BackupStageSync::finish");
        holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zookeeper);
            createFinishNode(zookeeper);
            removeAliveNode(zookeeper);
        });

        finished = true;
    }
    catch (...)
    {
        failed_to_finish = true;
        throw;
    }
}


bool BackupCoordinationStageSync::tryFinish() noexcept
{
    if (finished)
        return true;

    if (failed_to_finish)
        return false;

    try
    {
        stopWatchingThread();

        auto holder = with_retries.createRetriesControlHolder("BackupStageSync::tryFinish", {.error_handling = true});
        holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zookeeper);
            createFinishNode(zookeeper);
            removeAliveNode(zookeeper);
        });

        finished = true;
        return true;
    }
    catch (...)
    {
        /// retryLoop() inside createFinishNode() and removeAliveNode() has already logged this exception.
        failed_to_finish = true;
        return false;
    }
}


void BackupCoordinationStageSync::createFinishNode(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper)
{
    if (zookeeper->exists(finish_node_path))
        return;

    std::optional<size_t> num_hosts;
    int num_hosts_version = -1;

    for (size_t attempt_no = 1; attempt_no <= max_attempts_after_bad_version; ++attempt_no)
    {
        if (!num_hosts)
        {
            Coordination::Stat stat;
            num_hosts = parseFromString<size_t>(zookeeper->get(num_hosts_node_path, &stat));
            num_hosts_version = stat.version;
        }

        Coordination::Requests requests;
        requests.reserve(2);
        requests.emplace_back(zkutil::makeCreateRequest(finish_node_path, "", zkutil::CreateMode::Persistent));
        requests.emplace_back(zkutil::makeSetRequest(num_hosts_node_path, toString(*num_hosts - 1), num_hosts_version));

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(requests, responses);

        if (!((code == Coordination::Error::ZOK) || (code == Coordination::Error::ZBADVERSION)))
            zkutil::KeeperMultiException::check(code, requests, responses);

        if (code == Coordination::Error::ZOK)
        {
            --*num_hosts;
            String hosts_left_desc = ((*num_hosts == 0) ? "no hosts left" : fmt::format("{} hosts left", *num_hosts));
            LOG_INFO(log, "Created the 'finish' node in ZooKeeper for {}, {}", current_host_desc, hosts_left_desc);
            return;
        }

        bool will_try_again = (attempt_no < max_attempts_after_bad_version);

        LOG_TRACE(log, "Other host changed the 'num_hosts' node in ZooKeeper (attempt #{}){}", attempt_no,
                  (will_try_again ? ", will try again" : ""));
        num_hosts.reset(); /// need to reread 'num_hosts' again
    }

    throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                    "Couldn't create the 'finish' node for {} after {} attempts",
                    current_host_desc, max_attempts_after_bad_version);
}


void BackupCoordinationStageSync::waitHostsFinish(const Strings & hosts) const
{
    std::unique_lock lock{mutex};

    /// TSA_NO_THREAD_SAFETY_ANALYSIS is here because Clang Thread Safety Analysis doesn't understand std::unique_lock.
    auto check_if_hosts_finish = [&]() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return checkIfHostsFinish(hosts, /* throw_if_error = */ true);
    };

    state_changed.wait(lock, [&] { return check_if_hosts_finish(); });
}


bool BackupCoordinationStageSync::tryWaitHostsFinish(const Strings & hosts) const noexcept
{
    try
    {
        std::unique_lock lock{mutex};

        /// TSA_NO_THREAD_SAFETY_ANALYSIS is here because Clang Thread Safety Analysis doesn't understand std::unique_lock.
        auto check_if_hosts_finish = [&]() TSA_NO_THREAD_SAFETY_ANALYSIS
        {
            return checkIfHostsFinish(hosts, /* throw_if_error = */ false);
        };

        if (error_handling_timeout.count() != 0)
        {
            return state_changed.wait_for(lock, error_handling_timeout, [&] { return check_if_hosts_finish(); });
        }
        else
        {
            state_changed.wait(lock, [&] { return check_if_hosts_finish(); });
            return true;
        }
    }
    catch (...)
    {
        /// Normally checkIfHostsFinish() must not throw any exception with `throw_if_error = false`.
        tryLogCurrentException(log, fmt::format("Caught exception while waiting for other hosts to finish working on {}", operation_name));
        return false;
    }
}


bool BackupCoordinationStageSync::checkIfHostsFinish(const Strings & hosts, bool throw_if_error) const
{
    if (throw_if_error)
    {
        if (state.host_with_error)
            std::rethrow_exception(state.hosts.at(*state.host_with_error).exception);

        process_list_element->checkTimeLimit();
    }

    for (const auto & host : hosts)
    {
        auto it = state.hosts.find(host);

        bool never_connected = (it == state.hosts.end()) || !it->second.started;
        if (never_connected)
        {
            if (throw_if_error)
                throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Never connected to {}", getHostDesc(host));
            else
                continue;
        }

        const HostInfo & host_info = it->second;
        if (host_info.finished)
            continue;

        if (host_info.disconnected_too_long)
        {
            chassert(host_info.started && !host_info.connected);
            if (throw_if_error)
            {
                auto disconnected_seconds = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - host_info.last_connection_time_monotonic);
                String last_connection_desc = fmt::format(", last time the host was connected at {}", host_info.last_connection_time);
                throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                    "Lost connection to {} for {} which is more than the threshold for the failure set to {}{}",
                    getHostDesc(host), disconnected_seconds, failure_after_host_disconnected_for_seconds, last_connection_desc);
            }
            else
                continue;
        }

        LOG_TRACE(log, "Waiting for {} to finish working on {}", getHostDesc(host), operation_name);
        return false;
    }

    return true;
}

}
