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

    /// Empty string as the current host is used to mark the initiator of a BACKUP ON CLUSTER or RESTORE ON CLUSTER query.
    const constexpr std::string_view kInitiator;
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


BackupCoordinationStageSync::BackupCoordinationStageSync(
        bool is_restore_,
        const String & zookeeper_path_,
        const String & current_host_,
        const Strings & all_hosts_,
        bool allow_concurrency_,
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
    , with_retries(with_retries_)
    , schedule(schedule_)
    , process_list_element(process_list_element_)
    , log(log_)
    , failure_after_host_disconnected_for_seconds(with_retries.getKeeperSettings().failure_after_host_disconnected_for_seconds)
    , finish_timeout_after_error(with_retries.getKeeperSettings().finish_timeout_after_error)
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

    initializeState();
    createRootNodes();

    try
    {
        createStartAndAliveNodes();
        startWatchingThread();
    }
    catch (...)
    {
        trySetError(std::current_exception());
        tryFinishImpl();
        throw;
    }
}


BackupCoordinationStageSync::~BackupCoordinationStageSync()
{
    tryFinishImpl();
}


void BackupCoordinationStageSync::initializeState()
{
    std::lock_guard lock{mutex};
    auto now = std::chrono::system_clock::now();
    auto monotonic_now = std::chrono::steady_clock::now();

    for (const String & host : all_hosts)
        state.hosts.emplace(host, HostInfo{.host = host, .last_connection_time = now, .last_connection_time_monotonic = monotonic_now});
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


void BackupCoordinationStageSync::createRootNodes()
{
    auto holder = with_retries.createRetriesControlHolder("BackupStageSync::createRootNodes", WithRetries::kInitialization);
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
    auto holder = with_retries.createRetriesControlHolder("BackupStageSync::createStartAndAliveNodes", WithRetries::kInitialization);
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

        String serialized_error;
        if (zookeeper->tryGet(error_node_path, serialized_error))
        {
            auto [exception, host] = parseErrorNode(serialized_error);
            if (exception)
                std::rethrow_exception(exception);
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
        requests.reserve(6);

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

        requests.emplace_back(zkutil::makeCreateRequest(start_node_path, std::to_string(kCurrentVersion), zkutil::CreateMode::Persistent));
        requests.emplace_back(zkutil::makeCreateRequest(alive_node_path, "", zkutil::CreateMode::Ephemeral));

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(requests, responses);

        if (code == Coordination::Error::ZOK)
        {
            LOG_INFO(log, "Created start node #{} in ZooKeeper for {} (coordination version: {})",
                     num_hosts.value_or(0) + 1, current_host_desc, kCurrentVersion);
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
            /// needs another attempt
        }
        else if ((responses.size() > stage_node_path_pos) &&
            (responses[stage_node_path_pos]->error == Coordination::Error::ZNODEEXISTS))
        {
            show_error_before_next_attempt(fmt::format("Node {} in ZooKeeper already exists", stage_node_path));
            /// needs another attempt
        }
        else if ((responses.size() > num_hosts_node_path_pos) && num_hosts &&
            (responses[num_hosts_node_path_pos]->error == Coordination::Error::ZBADVERSION))
        {
            show_error_before_next_attempt("Other host changed the 'num_hosts' node in ZooKeeper");
            num_hosts.reset(); /// needs to reread 'num_hosts' again
        }
        else if ((responses.size() > num_hosts_node_path_pos) && num_hosts &&
            (responses[num_hosts_node_path_pos]->error == Coordination::Error::ZNONODE))
        {
            show_error_before_next_attempt("Other host removed the 'num_hosts' node in ZooKeeper");
            num_hosts.reset(); /// needs to reread 'num_hosts' again
        }
        else if ((responses.size() > num_hosts_node_path_pos) && !num_hosts &&
            (responses[num_hosts_node_path_pos]->error == Coordination::Error::ZNODEEXISTS))
        {
            show_error_before_next_attempt("Other host created the 'num_hosts' node in ZooKeeper");
            /// needs another attempt
        }
        else if ((responses.size() > alive_tracker_node_path_pos) &&
            (responses[alive_tracker_node_path_pos]->error == Coordination::Error::ZBADVERSION))
        {
            show_error_before_next_attempt("Concurrent backup or restore changed some 'alive' nodes in ZooKeeper");
            check_concurrency = true; /// needs to recheck for concurrency again
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
    while (!should_stop_watching_thread)
    {
        try
        {
            /// Check if the current BACKUP or RESTORE command is already cancelled.
            checkIfQueryCancelled();

            /// Reset the `connected` flag for each host, we'll set them to true again after we find the 'alive' nodes.
            resetConnectedFlag();

            /// Recreate the 'alive' node if necessary and read a new state from ZooKeeper.
            auto holder = with_retries.createRetriesControlHolder("BackupStageSync::watchingThread");
            auto & zookeeper = holder.faulty_zookeeper;
            with_retries.renewZooKeeper(zookeeper);

            if (should_stop_watching_thread)
                return;

            /// Recreate the 'alive' node if it was removed.
            createAliveNode(zookeeper);

            /// Reads the current state from nodes in ZooKeeper.
            readCurrentState(zookeeper);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caugth exception while watching");
        }

        try
        {
            /// Cancel the query if there is an error on another host or if some host was disconnected too long.
            cancelQueryIfError();
            cancelQueryIfDisconnectedTooLong();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caugth exception while checking if the query should be cancelled");
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


void BackupCoordinationStageSync::resetConnectedFlag()
{
    std::lock_guard lock{mutex};
    for (auto & [_, host_info] : state.hosts)
        host_info.connected = false;
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

        /// Log all changes in zookeeper nodes in the "stage" folder to make debugging easier.
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
    for (const auto & zk_node : new_zk_nodes)
    {
        if (zk_node == "error")
        {
            if (!new_state.host_with_error)
            {
                String serialized_error = zookeeper->get(error_node_path);
                auto [exception, host] = parseErrorNode(serialized_error);
                if (auto * host_info = get_host_info(host))
                {
                    host_info->exception = exception;
                    new_state.host_with_error = host;
                }
            }
        }
        else if (zk_node.starts_with("started|"))
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


std::pair<std::exception_ptr, String> BackupCoordinationStageSync::parseErrorNode(const String & error_node_contents)
{
    ReadBufferFromOwnString buf{error_node_contents};
    String host;
    readStringBinary(host, buf);
    auto exception = std::make_exception_ptr(readException(buf, fmt::format("Got error from {}", getHostDesc(host))));
    return {exception, host};
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


void BackupCoordinationStageSync::cancelQueryIfDisconnectedTooLong()
{
    std::exception_ptr exception;

    {
        std::lock_guard lock{mutex};
        if (state.cancelled || state.host_with_error || ((failure_after_host_disconnected_for_seconds.count() == 0)))
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

        if (!exception)
            return;

        state.cancelled = true;
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


bool BackupCoordinationStageSync::trySetError(std::exception_ptr exception) noexcept
{
    try
    {
        std::rethrow_exception(exception);
    }
    catch (const Exception & e)
    {
        return trySetError(e);
    }
    catch (...)
    {
        return trySetError(Exception(getCurrentExceptionMessageAndPattern(true, true), getCurrentExceptionCode()));
    }
}


bool BackupCoordinationStageSync::trySetError(const Exception & exception)
{
    try
    {
        setError(exception);
        return true;
    }
    catch (...)
    {
        return false;
    }
}


void BackupCoordinationStageSync::setError(const Exception & exception)
{
    /// Most likely this exception has been already logged so here we're logging it without stacktrace.
    String exception_message = getExceptionMessage(exception, /* with_stacktrace= */ false, /* check_embedded_stacktrace= */ true);
    LOG_INFO(log, "Sending exception from {} to other hosts: {}", current_host_desc, exception_message);

    auto holder = with_retries.createRetriesControlHolder("BackupStageSync::setError", WithRetries::kErrorHandling);

    holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zookeeper);

        WriteBufferFromOwnString buf;
        writeStringBinary(current_host, buf);
        writeException(exception, buf, true);
        auto code = zookeeper->tryCreate(error_node_path, buf.str(), zkutil::CreateMode::Persistent);

        if (code == Coordination::Error::ZOK)
        {
            LOG_TRACE(log, "Sent exception from {} to other hosts", current_host_desc);
        }
        else if (code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_INFO(log, "An error has been already assigned for this {}", operation_name);
        }
        else
        {
            throw zkutil::KeeperException::fromPath(code, error_node_path);
        }
    });
}


Strings BackupCoordinationStageSync::waitForHostsToReachStage(const String & stage_to_wait, const Strings & hosts, std::optional<std::chrono::milliseconds> timeout) const
{
    Strings results;
    results.resize(hosts.size());

    std::unique_lock lock{mutex};

    /// TSA_NO_THREAD_SAFETY_ANALYSIS is here because Clang Thread Safety Analysis doesn't understand std::unique_lock.
    auto check_if_hosts_ready = [&](bool time_is_out) TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return checkIfHostsReachStage(hosts, stage_to_wait, time_is_out, timeout, results);
    };

    if (timeout)
    {
        if (!state_changed.wait_for(lock, *timeout, [&] { return check_if_hosts_ready(/* time_is_out = */ false); }))
            check_if_hosts_ready(/* time_is_out = */ true);
    }
    else
    {
        state_changed.wait(lock, [&] { return check_if_hosts_ready(/* time_is_out = */ false); });
    }

    return results;
}


bool BackupCoordinationStageSync::checkIfHostsReachStage(
    const Strings & hosts,
    const String & stage_to_wait,
    bool time_is_out,
    std::optional<std::chrono::milliseconds> timeout,
    Strings & results) const
{
    if (should_stop_watching_thread)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "finish() was called while waiting for a stage");

    process_list_element->checkTimeLimit();

    for (size_t i = 0; i != hosts.size(); ++i)
    {
        const String & host = hosts[i];
        auto it = state.hosts.find(host);

        if (it == state.hosts.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "waitForHostsToReachStage() was called for unexpected {}, all hosts are {}", getHostDesc(host), getHostsDesc(all_hosts));

        const HostInfo & host_info = it->second;
        auto stage_it = host_info.stages.find(stage_to_wait);
        if (stage_it != host_info.stages.end())
        {
            results[i] = stage_it->second;
            continue;
        }

        if (host_info.finished)
        {
            throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                            "{} finished without coming to stage {}", getHostDesc(host), stage_to_wait);
        }

        String host_status;
        if (!host_info.started)
            host_status = fmt::format(": the host hasn't started working on this {} yet", operation_name);
        else if (!host_info.connected)
            host_status = fmt::format(": the host is currently disconnected, last connection was at {}", host_info.last_connection_time);

        if (!time_is_out)
        {
            LOG_TRACE(log, "Waiting for {} to reach stage {}{}", getHostDesc(host), stage_to_wait, host_status);
            return false;
        }
        else
        {
            throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                            "Waited longer than timeout {} for {} to reach stage {}{}",
                            *timeout, getHostDesc(host), stage_to_wait, host_status);
        }
    }

    LOG_INFO(log, "Hosts {} reached stage {}", getHostsDesc(hosts), stage_to_wait);
    return true;
}


void BackupCoordinationStageSync::finish(bool & other_hosts_also_finished)
{
    tryFinishImpl(other_hosts_also_finished, /* throw_if_error = */ true, /* retries_kind = */ WithRetries::kNormal);
}


bool BackupCoordinationStageSync::tryFinishAfterError(bool & other_hosts_also_finished) noexcept
{
    return tryFinishImpl(other_hosts_also_finished, /* throw_if_error = */ false, /* retries_kind = */ WithRetries::kErrorHandling);
}


bool BackupCoordinationStageSync::tryFinishImpl()
{
    bool other_hosts_also_finished;
    return tryFinishAfterError(other_hosts_also_finished);
}


bool BackupCoordinationStageSync::tryFinishImpl(bool & other_hosts_also_finished, bool throw_if_error, WithRetries::Kind retries_kind)
{
    auto get_value_other_hosts_also_finished = [&] TSA_REQUIRES(mutex)
    {
        other_hosts_also_finished = true;
        for (const auto & [host, host_info] : state.hosts)
        {
            if ((host != current_host) && !host_info.finished)
                other_hosts_also_finished = false;
        }
    };

    {
        std::lock_guard lock{mutex};
        if (finish_result.succeeded)
        {
            get_value_other_hosts_also_finished();
            return true;
        }
        if (finish_result.exception)
        {
            if (throw_if_error)
                std::rethrow_exception(finish_result.exception);
            return false;
        }
    }

    try
    {
        stopWatchingThread();

        auto holder = with_retries.createRetriesControlHolder("BackupStageSync::finish", retries_kind);
        holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zookeeper);
            createFinishNodeAndRemoveAliveNode(zookeeper);
        });

        std::lock_guard lock{mutex};
        finish_result.succeeded = true;
        get_value_other_hosts_also_finished();
        return true;
    }
    catch (...)
    {
        LOG_TRACE(log, "Caught exception while creating the 'finish' node for {}: {}",
            current_host_desc,
            getCurrentExceptionMessage(/* with_stacktrace= */ false, /* check_embedded_stacktrace= */ true));

        std::lock_guard lock{mutex};
        finish_result.exception = std::current_exception();
        if (throw_if_error)
            throw;
        return false;
    }
}


void BackupCoordinationStageSync::createFinishNodeAndRemoveAliveNode(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper)
{
    if (zookeeper->exists(finish_node_path))
        return;

    /// If the initiator of the query has that old version then it doesn't expect us to create the 'finish' node and moreover
    /// the initiator can start removing all the nodes immediately after all hosts report about reaching the "completed" status.
    /// So to avoid weird errors in the logs we won't create the 'finish' node if the initiator of the query has that old version.
    if ((getInitiatorVersion() == kVersionWithoutFinishNode) && (current_host != kInitiator))
    {
        LOG_INFO(log, "Skipped creating the 'finish' node because the initiator uses outdated version {}", getInitiatorVersion());
        return;
    }

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
        requests.reserve(3);

        requests.emplace_back(zkutil::makeCreateRequest(finish_node_path, "", zkutil::CreateMode::Persistent));

        size_t num_hosts_node_path_pos = requests.size();
        requests.emplace_back(zkutil::makeSetRequest(num_hosts_node_path, toString(*num_hosts - 1), num_hosts_version));

        size_t alive_node_path_pos = static_cast<size_t>(-1);
        if (zookeeper->exists(alive_node_path))
        {
            alive_node_path_pos = requests.size();
            requests.emplace_back(zkutil::makeRemoveRequest(alive_node_path, -1));
        }

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(requests, responses);

        if (code == Coordination::Error::ZOK)
        {
            --*num_hosts;
            String hosts_left_desc = ((*num_hosts == 0) ? "no hosts left" : fmt::format("{} hosts left", *num_hosts));
            LOG_INFO(log, "Created the 'finish' node in ZooKeeper for {}, {}", current_host_desc, hosts_left_desc);
            return;
        }

        auto show_error_before_next_attempt = [&](const String & message)
        {
            bool will_try_again = (attempt_no < max_attempts_after_bad_version);
            LOG_TRACE(log, "{} (attempt #{}){}", message, attempt_no, will_try_again ? ", will try again" : "");
        };

        if ((responses.size() > num_hosts_node_path_pos) &&
            (responses[num_hosts_node_path_pos]->error == Coordination::Error::ZBADVERSION))
        {
            show_error_before_next_attempt("Other host changed the 'num_hosts' node in ZooKeeper");
            num_hosts.reset(); /// needs to reread 'num_hosts' again
        }
        else if ((responses.size() > alive_node_path_pos) &&
            (responses[alive_node_path_pos]->error == Coordination::Error::ZNONODE))
        {
            show_error_before_next_attempt(fmt::format("Node {} in ZooKeeper doesn't exist", alive_node_path_pos));
            /// needs another attempt
        }
        else
        {
            zkutil::KeeperMultiException::check(code, requests, responses);
        }
    }

    throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                    "Couldn't create the 'finish' node for {} after {} attempts",
                    current_host_desc, max_attempts_after_bad_version);
}


int BackupCoordinationStageSync::getInitiatorVersion() const
{
    std::lock_guard lock{mutex};
    auto it = state.hosts.find(String{kInitiator});
    if (it == state.hosts.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no initiator of this {} query, it's a bug", operation_name);
    const HostInfo & host_info = it->second;
    return host_info.version;
}


void BackupCoordinationStageSync::waitForOtherHostsToFinish() const
{
    tryWaitForOtherHostsToFinishImpl(/* reason = */ "", /* throw_if_error = */ true, /* timeout = */ {});
}


bool BackupCoordinationStageSync::tryWaitForOtherHostsToFinishAfterError() const noexcept
{
    std::optional<std::chrono::seconds> timeout;
    if (finish_timeout_after_error.count() != 0)
        timeout = finish_timeout_after_error;

    String reason = fmt::format("{} needs other hosts to finish before cleanup", current_host_desc);
    return tryWaitForOtherHostsToFinishImpl(reason, /* throw_if_error = */ false, timeout);
}


bool BackupCoordinationStageSync::tryWaitForOtherHostsToFinishImpl(const String & reason, bool throw_if_error, std::optional<std::chrono::seconds> timeout) const
{
    std::unique_lock lock{mutex};

    /// TSA_NO_THREAD_SAFETY_ANALYSIS is here because Clang Thread Safety Analysis doesn't understand std::unique_lock.
    auto check_if_other_hosts_finish = [&](bool time_is_out) TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return checkIfOtherHostsFinish(reason, throw_if_error, time_is_out, timeout);
    };

    if (timeout)
    {
        if (state_changed.wait_for(lock, *timeout, [&] { return check_if_other_hosts_finish(/* time_is_out = */ false); }))
            return true;
        return check_if_other_hosts_finish(/* time_is_out = */ true);
    }
    else
    {
        state_changed.wait(lock, [&] { return check_if_other_hosts_finish(/* time_is_out = */ false); });
        return true;
    }
}


bool BackupCoordinationStageSync::checkIfOtherHostsFinish(const String & reason, bool throw_if_error, bool time_is_out, std::optional<std::chrono::milliseconds> timeout) const
{
    if (should_stop_watching_thread)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "finish() was called while waiting for other hosts to finish");

    if (throw_if_error)
        process_list_element->checkTimeLimit();

    for (const auto & [host, host_info] : state.hosts)
    {
        if ((host == current_host) || host_info.finished)
            continue;

        String host_status;
        if (!host_info.started)
            host_status = fmt::format(": the host hasn't started working on this {} yet", operation_name);
        else if (!host_info.connected)
            host_status = fmt::format(": the host is currently disconnected, last connection was at {}", host_info.last_connection_time);

        if (!time_is_out)
        {
            String reason_text = reason.empty() ? "" : (" because " + reason);
            LOG_TRACE(log, "Waiting for {} to finish{}{}", getHostDesc(host), reason_text, host_status);
            return false;
        }
        else
        {
            String reason_text = reason.empty() ? "" : fmt::format(" (reason of waiting: {})", reason);
            if (!throw_if_error)
            {
                LOG_INFO(log, "Waited longer than timeout {} for {} to finish{}{}",
                          *timeout, getHostDesc(host), host_status, reason_text);
                return false;
            }
            else
            {
                throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                                "Waited longer than timeout {} for {} to finish{}{}",
                                *timeout, getHostDesc(host), host_status, reason_text);
            }
        }
    }

    LOG_TRACE(log, "Other hosts finished working on this {}", operation_name);
    return true;
}

}
