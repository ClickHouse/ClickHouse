#pragma once

#include <Common/config.h>
#include "config_core.h"

#if USE_NURAFT

#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <functional>
#include <Coordination/KeeperServer.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/Keeper4LWInfo.h>
#include <Coordination/KeeperConnectionStats.h>

namespace DB
{
using ZooKeeperResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr & response)>;

/// Highlevel wrapper for ClickHouse Keeper.
/// Process user requests via consensus and return responses.
class KeeperDispatcher
{
private:
    mutable std::mutex push_request_mutex;

    using RequestsQueue = ConcurrentBoundedQueue<KeeperStorage::RequestForSession>;
    using SessionToResponseCallback = std::unordered_map<int64_t, ZooKeeperResponseCallback>;
    using UpdateConfigurationQueue = ConcurrentBoundedQueue<ConfigUpdateAction>;

    /// Size depends on coordination settings
    std::unique_ptr<RequestsQueue> requests_queue;
    ResponsesQueue responses_queue;
    SnapshotsQueue snapshots_queue{1};

    /// More than 1k updates is definitely misconfiguration.
    UpdateConfigurationQueue update_configuration_queue{1000};

    std::atomic<bool> shutdown_called{false};

    mutable std::mutex session_to_response_callback_mutex;
    /// These two maps looks similar, but serves different purposes.
    /// The first map is subscription map for normal responses like
    /// (get, set, list, etc.). Dispatcher determines callback for each response
    /// using session id from this map.
    SessionToResponseCallback session_to_response_callback;

    /// But when client connects to the server for the first time it doesn't
    /// have session_id. It request it from server. We give temporary
    /// internal id for such requests just to much client with its response.
    SessionToResponseCallback new_session_id_response_callback;

    /// Reading and batching new requests from client handlers
    ThreadFromGlobalPool request_thread;
    /// Pushing responses to clients client handlers
    /// using session_id.
    ThreadFromGlobalPool responses_thread;
    /// Cleaning old dead sessions
    ThreadFromGlobalPool session_cleaner_thread;
    /// Dumping new snapshots to disk
    ThreadFromGlobalPool snapshot_thread;
    /// Apply or wait for configuration changes
    ThreadFromGlobalPool update_configuration_thread;

    /// RAFT wrapper.
    std::unique_ptr<KeeperServer> server;

    KeeperConnectionStats keeper_stats;

    KeeperConfigurationAndSettingsPtr configuration_and_settings;

    Poco::Logger * log;

    /// Counter for new session_id requests.
    std::atomic<int64_t> internal_session_id_counter{0};

    /// Thread put requests to raft
    void requestThread();
    /// Thread put responses for subscribed sessions
    void responseThread();
    /// Thread clean disconnected sessions from memory
    void sessionCleanerTask();
    /// Thread create snapshots in the background
    void snapshotThread();
    /// Thread apply or wait configuration changes from leader
    void updateConfigurationThread();

    void setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response);

    /// Add error responses for requests to responses queue.
    /// Clears requests.
    void addErrorResponses(const KeeperStorage::RequestsForSessions & requests_for_sessions, Coordination::Error error);

    /// Forcefully wait for result and sets errors if something when wrong.
    /// Clears both arguments
    void forceWaitAndProcessResult(RaftAppendResult & result, KeeperStorage::RequestsForSessions & requests_for_sessions);

public:
    /// Just allocate some objects, real initialization is done by `intialize method`
    KeeperDispatcher();

    /// Call shutdown
    ~KeeperDispatcher();

    /// Initialization from config.
    /// standalone_keeper -- we are standalone keeper application (not inside clickhouse server)
    void initialize(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper, bool start_async);

    void startServer();

    bool checkInit() const
    {
        return server && server->checkInit();
    }

    /// Is server accepting requests, i.e. connected to the cluster
    /// and achieved quorum
    bool isServerActive() const;

    /// Registered in ConfigReloader callback. Add new configuration changes to
    /// update_configuration_queue. Keeper Dispatcher apply them asynchronously.
    void updateConfiguration(const Poco::Util::AbstractConfiguration & config);

    /// Shutdown internal keeper parts (server, state machine, log storage, etc)
    void shutdown();

    void forceRecovery();

    /// Put request to ClickHouse Keeper
    bool putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id);

    /// Get new session ID
    int64_t getSessionID(int64_t session_timeout_ms);

    /// Register session and subscribe for responses with callback
    void registerSession(int64_t session_id, ZooKeeperResponseCallback callback);

    /// Call if we don't need any responses for this session no more (session was expired)
    void finishSession(int64_t session_id);

    /// Invoked when a request completes.
    void updateKeeperStatLatency(uint64_t process_time_ms);

    /// Are we leader
    bool isLeader() const
    {
        return server->isLeader();
    }

    bool hasLeader() const
    {
        return server->isLeaderAlive();
    }

    bool isObserver() const
    {
        return server->isObserver();
    }

    uint64_t getLogDirSize() const;

    uint64_t getSnapDirSize() const;

    /// Request statistics such as qps, latency etc.
    KeeperConnectionStats & getKeeperConnectionStats()
    {
        return keeper_stats;
    }

    Keeper4LWInfo getKeeper4LWInfo() const;

    const KeeperStateMachine & getStateMachine() const
    {
        return *server->getKeeperStateMachine();
    }

    const KeeperConfigurationAndSettingsPtr & getKeeperConfigurationAndSettings() const
    {
        return configuration_and_settings;
    }

    void incrementPacketsSent()
    {
        keeper_stats.incrementPacketsSent();
    }

    void incrementPacketsReceived()
    {
        keeper_stats.incrementPacketsReceived();
    }

    void resetConnectionStats()
    {
        keeper_stats.reset();
    }
};

}

#endif
