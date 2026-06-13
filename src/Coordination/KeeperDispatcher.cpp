#include <Coordination/KeeperDispatcher.h>

#if USE_NURAFT

#include <Common/ProfiledLocks.h>
#include <libnuraft/async.hxx>

#include <Poco/Path.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <base/hex.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/OpenTelemetryTracingContext.h>
#include <Common/HistogramMetrics.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/setThreadName.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/checkStackSize.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/MemoryTracker.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>
#include <Coordination/KeeperCommon.h>
#include <Common/ZooKeeper/KeeperSpans.h>
#include <Interpreters/Context.h>
#include <Common/thread_local_rng.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperReconfiguration.h>

#include <IO/ReadHelpers.h>
#include <Disks/IDisk.h>

#include <atomic>
#include <future>
#include <chrono>
#include <limits>
#include <string>

#include <fmt/ranges.h>

#if USE_JEMALLOC
#include <Common/Jemalloc.h>
#endif

namespace CurrentMetrics
{
    extern const Metric KeeperAliveConnections;
    extern const Metric KeeperOutstandingRequests;
}

namespace HistogramMetrics
{
    extern Metric & KeeperCurrentBatchSizeElements;
    extern Metric & KeeperCurrentBatchSizeBytes;
}

using namespace std::chrono_literals;

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsMilliseconds dead_session_check_period_ms;
    extern const CoordinationSettingsUInt64 max_request_queue_size;
    extern const CoordinationSettingsUInt64 max_requests_batch_bytes_size;
    extern const CoordinationSettingsUInt64 max_requests_batch_size;
    extern const CoordinationSettingsUInt64 max_read_batch_bytes_size;
    extern const CoordinationSettingsUInt64 max_read_batch_size;
    extern const CoordinationSettingsMilliseconds operation_timeout_ms;
    extern const CoordinationSettingsBool quorum_reads;
    extern const CoordinationSettingsMilliseconds session_shutdown_timeout;
    extern const CoordinationSettingsMilliseconds sleep_before_leader_change_ms;
    extern const CoordinationSettingsBool use_new_dispatcher;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int SYSTEM_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int ABORTED;
}

KeeperDispatcher::KeeperDispatcher()
    : server_config(std::make_shared<KeeperConfiguration>())
    , log(getLogger("KeeperDispatcher"))
{}

void KeeperDispatcher::initialize(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper, bool start_async, const MultiVersion<Macros>::Version & macros)
{
    LOG_DEBUG(log, "Initializing storage dispatcher");

    server_config = KeeperConfiguration::loadFromConfig(config, standalone_keeper);
    auto coordination_settings = std::make_shared<CoordinationSettings>();
    coordination_settings->loadFromConfig("keeper_server.coordination_settings", config);
    keeper_context = std::make_shared<KeeperContext>(standalone_keeper, std::move(coordination_settings));

    keeper_context->initialize(config, this);

    snapshot_thread = ThreadFromGlobalPool([this] { snapshotThread(); });

    snapshot_s3.startup(config, macros);

    server = std::make_unique<KeeperServer>(
        server_config,
        config,
        [this](KeeperResponseForSession response)
        {
            response.response->enqueue_ts = std::chrono::steady_clock::now();
            if (response.request)
                response.request->spans.maybeInitialize(KeeperSpan::DispatcherResponsesQueue, response.request->tracing_context.get());

            /// Special new session response.
            if (response.response->xid != Coordination::WATCH_XID && response.response->getOpNum() == Coordination::OpNum::SessionID)
                onSessionIDResponse(response.response);
            else if (dispatcher_old)
                dispatcher_old->onResponse(std::move(response));
            else
                dispatcher->onResponse(std::move(response));
        },
        snapshots_queue,
        keeper_context,
        snapshot_s3,
        [this](uint64_t /*log_idx*/, const KeeperRequestForSession & request_for_session)
        {
            if (dispatcher_old)
                dispatcher_old->onCommit(request_for_session);
            else
                dispatcher->onCommit(request_for_session);
        });

    bool new_dispatcher_response_thread_started = false;

    try
    {
        if (keeper_context->getCoordinationSettings()[CoordinationSetting::use_new_dispatcher])
        {
            dispatcher = std::make_unique<KeeperRequestDispatcher>(server.get());
            dispatcher->startupResponseThread();
            new_dispatcher_response_thread_started = true;
        }
        else
        {
            dispatcher_old = std::make_unique<KeeperRequestDispatcherOld>(server.get());
        }

        LOG_DEBUG(log, "Waiting server to initialize");
        server->startup(config, server_config->enable_ipv6);
        LOG_DEBUG(log, "Server initialized, waiting for quorum");

        if (!start_async)
        {
            server->waitInit();
            LOG_DEBUG(log, "Quorum initialized");
        }
        else
        {
            LOG_INFO(log, "Starting Keeper asynchronously, server will accept connections to Keeper when it will be ready");
        }

        if (dispatcher)
            dispatcher->startupDispatchThread(); // after `server->startup` to avoid using `raft_instance` before it exists
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        if (new_dispatcher_response_thread_started && dispatcher)
            dispatcher->shutdown(/*closed_all_connections=*/false);

        throw;
    }

    /// Start it after keeper server start
    session_cleaner_thread = ThreadFromGlobalPool([this] { sessionCleanerTask(); });

    update_configuration_thread = reconfigEnabled()
        ? ThreadFromGlobalPool([this] { clusterUpdateThread(); })
        : ThreadFromGlobalPool([this] { clusterUpdateWithReconfigDisabledThread(); });

    LOG_DEBUG(log, "Dispatcher initialized");
}

void KeeperDispatcher::shutdown(bool closed_all_connections)
{
    try
    {
        {
            if (!keeper_context || !keeper_context->setShutdownCalled())
                return;

            LOG_DEBUG(log, "Shutting down storage dispatcher");

            if (session_cleaner_thread.joinable())
                session_cleaner_thread.join();

            snapshots_queue.finish();
            if (snapshot_thread.joinable())
                snapshot_thread.join();

            cluster_update_queue.finish();
            if (update_configuration_thread.joinable())
                update_configuration_thread.join();
        }

        if (dispatcher_old)
            dispatcher_old->shutdown();
        if (dispatcher)
            dispatcher->shutdown(closed_all_connections);

        if (server)
            server->shutdown();

        snapshot_s3.shutdown();

        CurrentMetrics::set(CurrentMetrics::KeeperAliveConnections, 0);

    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    LOG_DEBUG(log, "Dispatcher shut down");
}

void KeeperDispatcher::snapshotThread()
{
    DB::setThreadName(ThreadName::KEEPER_SNAPSHOT);

    const auto & shutdown_called = keeper_context->isShutdownCalled();
    CreateSnapshotTask task;
    while (snapshots_queue.pop(task))
    {
        try
        {
            auto snapshot_file_info = task.create_snapshot(std::move(task.snapshot), /*execute_only_cleanup=*/shutdown_called);

            if (!snapshot_file_info)
                continue;

            chassert(snapshot_file_info->disk != nullptr);
            if (isLeader())
                snapshot_s3.uploadSnapshot(snapshot_file_info);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

bool KeeperDispatcher::putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, bool use_xid_64)
{
    if (dispatcher_old)
        return dispatcher_old->putRequest(request, session_id, use_xid_64);
    else
        return dispatcher->putRequest(request, session_id, use_xid_64);
}

void KeeperDispatcher::forceRecovery()
{
    server->forceRecovery();
}

KeeperDispatcher::~KeeperDispatcher()
{
    shutdown(false);
}

void KeeperDispatcher::registerSession(int64_t session_id, ZooKeeperResponseCallback callback)
{
    if (dispatcher_old)
        dispatcher_old->registerSession(session_id, std::move(callback));
    else
        dispatcher->registerSession(session_id, std::move(callback));
}

void KeeperDispatcher::sessionCleanerTask()
{
    const auto & shutdown_called = keeper_context->isShutdownCalled();
    while (true)
    {
        if (shutdown_called)
            return;

        try
        {
            /// Only leader node must check dead sessions
            if (server->checkInit() && isLeader())
            {
                auto dead_sessions = server->getDeadSessions();

                for (int64_t dead_session : dead_sessions)
                {
                    LOG_INFO(log, "Found dead session {}, will try to close it", dead_session);

                    /// Close session == send close request to raft server
                    auto request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
                    request->xid = Coordination::CLOSE_XID;

                    /// Remove session from live_sessions before pushing Close to the queue.
                    /// This gives the leader early filtering — stale requests for
                    /// this session are skipped as soon as the session expiry is detected,
                    /// before the Close even enters the queue.
                    /// Close requests are exempt from stale filtering, so the
                    /// Close will still pass through RAFT for ephemeral cleanup.
                    finishSession(dead_session);
                    putRequest(request, dead_session, /*use_xid_64=*/ false);
                    LOG_INFO(log, "Dead session close request pushed");
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        auto time_to_sleep = keeper_context->getCoordinationSettings()[CoordinationSetting::dead_session_check_period_ms].totalMilliseconds();
        std::this_thread::sleep_for(std::chrono::milliseconds(time_to_sleep));
    }
}

void KeeperDispatcher::finishSession(int64_t session_id)
{
    if (dispatcher_old)
        dispatcher_old->finishSession(session_id);
    else
        dispatcher->finishSession(session_id);
}

void KeeperDispatcher::onSessionIDResponse(const Coordination::ZooKeeperResponsePtr & response) noexcept
{
    chassert(response->getOpNum() == Coordination::OpNum::SessionID);
    const Coordination::ZooKeeperSessionIDResponse & session_id_resp = dynamic_cast<const Coordination::ZooKeeperSessionIDResponse &>(*response);
    if (session_id_resp.server_id != server->getServerID())
        return;

    std::lock_guard lock(new_session_id_mutex);

    auto it = new_session_id_requests.find(session_id_resp.internal_id);
    if (it == new_session_id_requests.end())
        return;

    if (response->error == Coordination::Error::ZOK)
        it->second.set_value(session_id_resp.session_id);
    else
        it->second.set_exception(
            std::make_exception_ptr(zkutil::KeeperException::fromMessage(response->error, "SessionID request failed with error")));

    new_session_id_requests.erase(it);
}

int64_t KeeperDispatcher::getSessionID(int64_t session_timeout_ms)
{
    /// New session id allocation is a special request, because we cannot process it in normal
    /// way: get request -> put to raft -> set response for registered callback.
    KeeperRequestForSession request_info;
    std::shared_ptr<Coordination::ZooKeeperSessionIDRequest> request = std::make_shared<Coordination::ZooKeeperSessionIDRequest>();
    /// Internal session id. It's a temporary number which is unique for each client on this server
    /// but can be same on different servers.
    request->internal_id = internal_session_id_counter.fetch_add(1);
    request->session_timeout_ms = session_timeout_ms;
    request->server_id = server->getServerID();

    request_info.request = request;
    using namespace std::chrono;
    request_info.time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    request_info.session_id = -1;

    std::future<int64_t> future;

    {
        std::lock_guard lock(new_session_id_mutex);
        auto [it, inserted] = new_session_id_requests.try_emplace(request->internal_id);
        chassert(inserted);
        future = it->second.get_future();
    }

    try
    {
        /// Push new session request to queue
        if (!putRequest(request, /*session_id=*/ -1, /*use_xid_64=*/ false))
            throw Exception(ErrorCodes::ABORTED, "Not issuing new session ID because of shutdown");
    }
    catch (...)
    {
        {
            std::lock_guard lock(new_session_id_mutex);
            new_session_id_requests.erase(request->internal_id);
        }
        throw;
    }

    if (future.wait_for(std::chrono::milliseconds(session_timeout_ms)) != std::future_status::ready)
    {
        {
            std::lock_guard lock(new_session_id_mutex);
            new_session_id_requests.erase(request->internal_id);
        }
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Cannot receive session id within session timeout");
    }

    /// Forcefully wait for request execution because we cannot process any other
    /// requests for this client until it get new session id.
    return future.get();
}

void KeeperDispatcher::clusterUpdateWithReconfigDisabledThread()
{
    const auto & shutdown_called = keeper_context->isShutdownCalled();
    while (!shutdown_called)
    {
        try
        {
            if (!server->checkInit())
            {
                LOG_INFO(log, "Server still not initialized, will not apply configuration until initialization finished");
                std::this_thread::sleep_for(5000ms);
                continue;
            }

            if (server->isRecovering())
            {
                LOG_INFO(log, "Server is recovering, will not apply configuration until recovery is finished");
                std::this_thread::sleep_for(5000ms);
                continue;
            }

            ClusterUpdateAction action;
            if (!cluster_update_queue.pop(action))
                break;

            /// We must wait this update from leader or apply it ourself (if we are leader)
            bool done = false;
            while (!done)
            {
                if (server->isRecovering())
                    break;

                if (shutdown_called)
                    return;

                if (isLeader())
                {
                    server->applyConfigUpdateWithReconfigDisabled(action);
                    done = true;
                }
                else if (done = server->waitForConfigUpdateWithReconfigDisabled(action); !done)
                    LOG_INFO(log,
                        "Cannot wait for configuration update, maybe we became leader "
                        "or maybe update is invalid, will try to wait one more time");
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void KeeperDispatcher::clusterUpdateThread()
{
    using enum KeeperServer::ConfigUpdateState;
    bool last_command_was_leader_change = false;
    const auto & shutdown_called = keeper_context->isShutdownCalled();
    while (!shutdown_called)
    {
        ClusterUpdateAction action;
        if (!cluster_update_queue.pop(action))
            return;

        if (const auto res = server->applyConfigUpdate(action, last_command_was_leader_change); res == Accepted)
            LOG_DEBUG(log, "Processing config update {}: accepted", action);
        else
        {
            last_command_was_leader_change = res == WaitBeforeChangingLeader;

            (void)cluster_update_queue.pushFront(action);
            LOG_DEBUG(log, "Processing config update {}: declined, backoff", action);

            std::this_thread::sleep_for(last_command_was_leader_change
                ? std::chrono::milliseconds(keeper_context->getCoordinationSettings()[CoordinationSetting::sleep_before_leader_change_ms].totalMilliseconds())
                : 50ms);
        }
    }
}

void KeeperDispatcher::pushClusterUpdates(ClusterUpdateActions && actions)
{
    if (keeper_context->isShutdownCalled()) return;
    for (auto && action : actions)
    {
        if (!cluster_update_queue.push(std::move(action)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot push configuration update");
        LOG_DEBUG(log, "Processing config update {}: pushed", action);
    }
}

bool KeeperDispatcher::reconfigEnabled() const
{
    return server->reconfigEnabled();
}

bool KeeperDispatcher::isServerActive() const
{
    return checkInit() && hasLeader() && !server->isRecovering();
}

void KeeperDispatcher::updateConfiguration(const Poco::Util::AbstractConfiguration & config, const MultiVersion<Macros>::Version & macros)
{
    auto diff = server->getRaftConfigurationDiff(config);

    if (diff.empty())
        LOG_TRACE(log, "Configuration update triggered, but nothing changed for Raft");
    else if (reconfigEnabled())
        LOG_WARNING(log,
            "Raft configuration changed, but keeper_server.enable_reconfiguration is on. "
            "This update will be ignored. Use \"reconfig\" instead");
    else if (diff.size() > 1)
        LOG_WARNING(log,
            "Configuration changed for more than one server ({}) from cluster, "
            "it's strictly not recommended", diff.size());
    else
        LOG_DEBUG(log, "Configuration change size ({})", diff.size());

    if (!reconfigEnabled())
        for (auto & change : diff)
            if (!cluster_update_queue.push(change))
                throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push configuration update to queue");

    snapshot_s3.updateS3Configuration(config, macros);

    keeper_context->updateKeeperMemorySoftLimit(config);

    auto new_settings = std::make_shared<CoordinationSettings>();
    new_settings->loadFromConfig("keeper_server.coordination_settings", config);
    keeper_context->updateSettings(new_settings);
}

void KeeperDispatcher::updateKeeperStatLatency(uint64_t process_time_ms, uint64_t subrequests)
{
    keeper_stats.updateLatency(process_time_ms, subrequests);
}

static uint64_t getTotalSize(const DiskPtr & disk, const std::string & path = "")
{
    checkStackSize();

    uint64_t size = 0;
    for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
    {
        if (disk->existsFile(it->path()))
            size += disk->getFileSize(it->path());
        else
            size += getTotalSize(disk, it->path());
    }

    return size;
}

uint64_t KeeperDispatcher::getLogDirSize() const
{
    auto log_disk = keeper_context->getLogDisk();
    auto size = getTotalSize(log_disk);

    auto latest_log_disk = keeper_context->getLatestLogDisk();
    if (log_disk != latest_log_disk)
        size += getTotalSize(latest_log_disk);

    return size;
}

uint64_t KeeperDispatcher::getSnapDirSize() const
{
    return getTotalSize(keeper_context->getSnapshotDisk());
}

Keeper4LWInfo KeeperDispatcher::getKeeper4LWInfo() const
{
    Keeper4LWInfo result = server->getPartiallyFilled4LWInfo();
    result.outstanding_requests_count
        = CurrentMetrics::values[CurrentMetrics::KeeperOutstandingRequests].load(std::memory_order_relaxed);
    result.alive_connections_count
        = CurrentMetrics::values[CurrentMetrics::KeeperAliveConnections].load(std::memory_order_relaxed);
    return result;
}


void KeeperDispatcher::executeClusterUpdateActionAndWaitConfigChange(const ClusterUpdateAction & action, KeeperDispatcher::ConfigCheckCallback check_callback, size_t max_action_wait_time_ms, int64_t retry_count)
{
    for (int64_t attempt = 0; attempt <= retry_count; ++attempt)
    {
        if (check_callback(server.get()))
        {
            LOG_DEBUG(log, "Configuration update {} already applied, nothing to do", action);
            return;
        }

        pushClusterUpdates({action});
        Stopwatch watch;
        LOG_DEBUG(log, "Waiting for configuration update {} to be applied, will wait for {} ms", action, max_action_wait_time_ms);
        while (watch.elapsedMilliseconds() < max_action_wait_time_ms)
        {
            if (keeper_context->isShutdownCalled())
                throw Exception(ErrorCodes::ABORTED, "Shutdown called, aborting configuration update");

            if (check_callback(server.get()))
            {
                LOG_DEBUG(log, "Configuration update {} applied successfully", action);
                return;
            }

            std::this_thread::sleep_for(1000ms);
        }
        LOG_INFO(log, "Timeout exceeded waiting for configuration update {} to be applied, attempt {}/{}", action, attempt + 1, retry_count + 1);
    }
    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded (with retries count {}) waiting for configuration update {} to happen", action, retry_count);
}

void KeeperDispatcher::checkReconfigCommandPreconditions(Poco::JSON::Object::Ptr reconfig_command)
{
    if (reconfig_command->has("preconditions"))
    {
        auto latest_config = server->getKeeperStateMachine()->getClusterConfig();
        auto preconditions = reconfig_command->getObject("preconditions");
        if (preconditions->has("members"))
        {
            std::unordered_set<int32_t> nodes;
            auto members = preconditions->getArray("members");
            for (unsigned int i = 0; i < members->size(); ++i)
                nodes.insert(members->getElement<int32_t>(i));

            auto servers_in_config = latest_config->get_servers();
            if (nodes.size() != servers_in_config.size())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Precondition failed: expected members size {}, actual {}",
                    nodes.size(),
                    servers_in_config.size());

            for (const auto & participant : servers_in_config)
            {
                if (!nodes.contains(participant->get_id()))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Precondition failed: member with with id {} found in cluster, but precondition members are {}",
                        participant->get_id(), fmt::join(nodes, ", "));
            }
        }

        if (preconditions->has("leaders"))
        {
            std::unordered_set<int32_t> leaders;
            auto leaders_array = preconditions->getArray("leaders");
            for (unsigned int i = 0; i < leaders_array->size(); ++i)
                leaders.insert(leaders_array->getElement<int32_t>(i));

            if (!server->isLeaderAlive())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Precondition failed: expected leader id {} but there is no leader currently",
                    fmt::join(leaders, ", "));

            if (!leaders.contains(static_cast<int32_t>(server->getLeaderID())))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Precondition failed: expected leader id {} does not match actual leader id {}",
                    fmt::join(leaders, ", "),
                    server->getLeaderID());
        }
    }

}
void KeeperDispatcher::checkReconfigCommandActions(Poco::JSON::Object::Ptr reconfig_command)
{
    if (!reconfig_command->has("actions"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Reconfigure command must have 'actions' field");

    auto actions = reconfig_command->getArray("actions");
    /// sever_id -> leader
    std::unordered_map<int32_t, bool> state_model;

    auto config = server->getKeeperStateMachine()->getClusterConfig();
    auto leader_id = server->getLeaderID();
    for (const auto & srv : config->get_servers())
    {
        int32_t server_id = srv->get_id();
        state_model[server_id] = server_id == leader_id;
    }
    auto is_leader = [&state_model](int32_t server_id) -> bool
    {
        auto it = state_model.find(server_id);
        if (it == state_model.end())
            return false;
        return it->second;
    };

    for (const auto & action_json : *actions)
    {
        const auto & action_obj = action_json.extract<Poco::JSON::Object::Ptr>();
        if (action_obj->has("remove_members"))
        {
            auto remove_members = action_obj->getArray("remove_members");
            for (const auto & member_id_json : *remove_members)
            {
                int32_t member_id = member_id_json.convert<int32_t>();
                if (member_id == server->getServerID())
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Reconfigure command cannot remove current server id {} from cluster because it would make other servers ignore all the commands from this one",
                        member_id);
                }
                if (is_leader(member_id))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Reconfigure command cannot remove leader server id {} from cluster, transfer leadership first and than remove it",
                        member_id);
                }
                state_model.erase(member_id);
            }
        }
        else if (action_obj->has("add_members"))
        {
            auto add_members = action_obj->getArray("add_members");
            for (const auto & member_id_json : *add_members)
            {
                const auto & member_obj = member_id_json.extract<Poco::JSON::Object::Ptr>();
                int32_t member_id = member_obj->getValue<int32_t>("id");
                if (state_model.contains(member_id))
                {
                    LOG_INFO(log,
                        "Reconfigure command cannot add server id {} to cluster because it's already present, will do nothing",
                        member_id);
                }
                state_model[member_id] = false;
            }
        }
        else if (action_obj->has("transfer_leadership"))
        {
            bool found = false;
            std::vector<int32_t> leader_ids;
            const auto & leader_ids_array = action_obj->getArray("transfer_leadership");
            if (leader_ids_array->size() == 0)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Reconfigure command 'transfer_leadership' action must have non-empty list of server ids");
            }

            /// Leader is going to be transferred, reset state model
            for (auto & [server_id, _] : state_model)
            {
                state_model[server_id] = false;
            }

            for (const auto & leader_id_json : *leader_ids_array)
            {
                int32_t current_leader_id = leader_id_json.convert<int32_t>();
                leader_ids.push_back(current_leader_id);
                if (state_model.contains(current_leader_id))
                {
                    found = true;
                    state_model[current_leader_id] = true;
                }
            }

            if (!found)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Reconfigure command cannot transfer leadership to any of server ids [{}] because they are not present in cluster",
                    fmt::join(leader_ids, ", "));
            }
        }
        else if (action_obj->has("set_priority"))
        {
            const auto & set_priority_obj = action_obj->getArray("set_priority");
            for (const auto & priority_change: *set_priority_obj)
            {
                const auto & priority_change_obj = priority_change.extract<Poco::JSON::Object::Ptr>();
                int member_id = priority_change_obj->getValue<int>("id");
                int priority = priority_change_obj->getValue<int>("priority");
                if (is_leader(member_id) && priority == 0)
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Reconfigure command cannot set priority 0 for server {}, because it can be potentially a leader", member_id);
                }
            }
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown action in reconfigure command");
        }
    }
}

Poco::JSON::Object::Ptr KeeperDispatcher::reconfigureClusterFromReconfigureCommand(Poco::JSON::Object::Ptr reconfig_command)
try
{
    if (!server->isLeaderAlive())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot reconfigure cluster because there is no active leader");

    checkReconfigCommandPreconditions(reconfig_command);
    checkReconfigCommandActions(reconfig_command);

    LOG_DEBUG(log, "Preconditions passed, executing reconfiguration");
    size_t max_action_wait_time_ms = reconfig_command->has("max_action_wait_time_ms")
        ? reconfig_command->getValue<size_t>("max_action_wait_time_ms")
        : 60000;

    size_t max_total_wait_time_ms = reconfig_command->has("max_total_wait_time_ms")
        ? reconfig_command->getValue<size_t>("max_total_wait_time_ms")
        : 300000;

    auto actions = reconfig_command->getArray("actions");
    Stopwatch total_watch;
    for (const auto & action_json : *actions)
    {

        UInt64 time_left_for_action = max_action_wait_time_ms;
        UInt64 time_spent = total_watch.elapsedMilliseconds();
        UInt64 time_left_total = max_total_wait_time_ms > time_spent ? max_total_wait_time_ms - time_spent : 0;
        UInt64 time_left = std::min(time_left_for_action, time_left_total);

        const auto & action_obj = action_json.extract<Poco::JSON::Object::Ptr>();

        int64_t retry_count = 1;
        if (action_obj->has("retry"))
            retry_count = std::min(retry_count, action_obj->getValue<int64_t>("retry"));

        if (action_obj->has("remove_members"))
        {
            auto remove_members = action_obj->getArray("remove_members");
            for (const auto & member_id_json : *remove_members)
            {
                int32_t member_id = member_id_json.convert<int32_t>();
                RemoveRaftServer remove_action{member_id};
                auto remove_callback = [this, member_id](KeeperServer * server_) -> bool
                {
                    auto config = server_->getKeeperStateMachine()->getClusterConfig();

                    if (config->get_server(member_id) == nullptr)
                    {
                        LOG_INFO(log, "Skip removing server id {} from cluster because it's not present in current configuration: {}",
                            member_id, serializeClusterConfig(config));
                        return true;
                    }
                    else
                    {
                        LOG_INFO(
                            log, "Waiting for removing server id {} from cluster configuration: {}",
                            member_id, serializeClusterConfig(config));
                        return false;
                    }
                };
                executeClusterUpdateActionAndWaitConfigChange(remove_action, std::move(remove_callback), time_left, retry_count);
            }
        }
        else if (action_obj->has("add_members"))
        {
            auto add_members = action_obj->getArray("add_members");
            for (const auto & member_id_json : *add_members)
            {
                const auto & member_obj = member_id_json.extract<Poco::JSON::Object::Ptr>();
                int member_id = member_obj->getValue<int32_t>("id");
                std::string endpoint = member_obj->getValue<std::string>("endpoint");
                bool learner = member_obj->has("learner") ? member_obj->getValue<bool>("learner") : false;
                int priority = member_obj->has("priority") ? member_obj->getValue<int>("priority") : 1;
                AddRaftServer add_action({RaftServerConfig{member_id, endpoint, learner, priority}});
                auto add_callback = [this, member_id](KeeperServer * server_) -> bool
                {
                    auto config = server_->getKeeperStateMachine()->getClusterConfig();
                    if (config->get_server(member_id) != nullptr)
                    {
                        LOG_INFO(
                            log, "Skip adding server id {} to cluster because it's already present in current configuration",
                            member_id);
                        return true;
                    }
                    else
                    {
                        LOG_INFO(
                            log, "Waiting for adding server id {} to cluster configuration",
                            member_id);
                        return false;
                    }
                };
                executeClusterUpdateActionAndWaitConfigChange(add_action, std::move(add_callback), time_left, retry_count);
            }
        }
        else if (action_obj->has("transfer_leadership"))
        {
            auto potential_laders = action_obj->getArray("transfer_leadership");
            std::vector<int32_t> leader_ids;
            for (const auto & leader_id_json : *potential_laders)
                leader_ids.push_back(leader_id_json.convert<int32_t>());
            std::shuffle(leader_ids.begin(), leader_ids.end(), thread_local_rng);

            int32_t new_leader_id = leader_ids.front();
            TransferLeadership transfer_action{new_leader_id};
            auto check_callback = [this, new_leader_id](KeeperServer * server_) -> bool
            {
                if (server_->getLeaderID() == new_leader_id)
                {
                    LOG_INFO(
                        log, "Leadership successfully transferred to server id {}",
                        new_leader_id);
                    return true;
                }
                else
                {
                LOG_INFO(
                    log, "Waiting for leadership transfer to server id {}. Current leader id is {}",
                    new_leader_id,
                    server_->getLeaderID());

                    return false;
                }
            };
            executeClusterUpdateActionAndWaitConfigChange(transfer_action, std::move(check_callback), time_left, retry_count);
        }
        else if (action_obj->has("set_priority"))
        {
            const auto & set_priority_obj = action_obj->getArray("set_priority");
            for (const auto & priority_change: *set_priority_obj)
            {
                const auto & priority_change_obj = priority_change.extract<Poco::JSON::Object::Ptr>();
                int member_id = priority_change_obj->getValue<int>("id");
                int priority = priority_change_obj->getValue<int>("priority");
                UpdateRaftServerPriority update_priority_action{member_id, priority};
                auto priority_callback = [this, member_id, priority](KeeperServer * server_) -> bool
                {
                    auto config = server_->getKeeperStateMachine()->getClusterConfig();
                    auto server_in_config = config->get_server(member_id);
                    if (server_in_config == nullptr)
                    {
                        LOG_INFO(
                            log, "Cannot set priority for server id {} because it's not present in current configuration",
                            member_id);
                        return true;
                    }
                    else if (server_in_config->get_priority() == priority)
                    {
                        LOG_INFO(
                            log, "Priority for server id {} successfully changed to {}", member_id, priority);
                        return true;
                    }
                    else
                    {
                        LOG_INFO(
                            log,
                            "Waiting for setting priority {} for server id {} in cluster configuration, current {}", priority, member_id, server_in_config->get_priority());
                        return false;
                    }
                };
                executeClusterUpdateActionAndWaitConfigChange(update_priority_action, std::move(priority_callback), time_left, retry_count);
            }
        }
        else
        {
            std::stringstream ss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            action_obj->stringify(ss);
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown reconfigure action {}", ss.str());
        }
    }

    LOG_DEBUG(log, "Reconfiguration successfully applied");
    Poco::JSON::Object::Ptr result = new Poco::JSON::Object();
    result->set("status", "ok");
    result->set("message", "Reconfiguration successfully applied");
    return result;
}
catch (...)
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
    Poco::JSON::Object::Ptr result = new Poco::JSON::Object();
    result->set("status", "error");
    result->set("message", getCurrentExceptionMessage(false));
    return result;
}


void KeeperDispatcher::cleanResources()
{
#if USE_JEMALLOC
    Jemalloc::purgeArenas();
#endif
}

void KeeperDispatcher::onResponseDeallocated(const Coordination::ZooKeeperResponse & response)
{
    if (dispatcher)
        dispatcher->onResponseDeallocated(response);
}

}

#endif
