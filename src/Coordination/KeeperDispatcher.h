#pragma once

#include <Interpreters/OpenTelemetrySpanLog.h>
#include "config.h"

#if USE_NURAFT

#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <functional>
#include <unordered_set>
#include <Coordination/KeeperServer.h>
#include <Coordination/Keeper4LWInfo.h>
#include <Coordination/KeeperConnectionStats.h>
#include <Coordination/KeeperSnapshotManagerS3.h>
#include <Common/MultiVersion.h>
#include <Common/Macros.h>
#include <Poco/JSON/Object.h>

namespace DB
{
/// Callback invoked by setResponse and finishSession to deliver responses to clients.
/// Must be safe for concurrent invocation: setResponse (from responseThread) and
/// finishSession (from dead session cleaner) may invoke copies of the same callback
/// concurrently for the same session.
using ZooKeeperResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request)>;

/// Highlevel wrapper for ClickHouse Keeper.
/// Process user requests via consensus and return responses.
class KeeperDispatcher
{
private:
    using RequestsQueue = ConcurrentBoundedQueue<KeeperRequestForSession>;
    using SessionToResponseCallback = std::unordered_map<int64_t, ZooKeeperResponseCallback>;
    using ClusterUpdateQueue = ConcurrentBoundedQueue<ClusterUpdateAction>;

    /// Size depends on coordination settings
    std::unique_ptr<RequestsQueue> requests_queue;
    ResponsesQueue responses_queue;
    SnapshotsQueue snapshots_queue{1};

    /// More than 1k updates is definitely misconfiguration.
    ClusterUpdateQueue cluster_update_queue{1000};

    mutable std::mutex live_sessions_mutex;
    std::unordered_set<int64_t> live_sessions;

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

    KeeperConfigurationPtr server_config;

    LoggerPtr log;

    /// Counter for new session_id requests.
    std::atomic<int64_t> internal_session_id_counter{0};

    KeeperSnapshotManagerS3 snapshot_s3;

    KeeperContextPtr keeper_context;

    /// Flag to signal TCP handlers that they should close connections.
    /// Set before the full shutdown() to allow handlers to exit promptly.
    std::atomic<bool> shutting_down{false};

    /// Thread put requests to raft
    void requestThread();
    /// Thread put responses for subscribed sessions
    void responseThread();
    /// Thread clean disconnected sessions from memory
    void sessionCleanerTask();
    /// Thread create snapshots in the background
    void snapshotThread();

    // TODO (myrrc) this should be removed once "reconfig" is stabilized
    void clusterUpdateWithReconfigDisabledThread();
    void clusterUpdateThread();

    /// Returns true if response was successfully sent to client, false if session doesn't exist on this node.
    bool setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request = nullptr);

    /// Add error responses for requests to responses queue.
    /// Clears requests.
    /// If may_have_dependent_reads is true, also looks at read_request_queue and adds error
    /// responses for any reads that were piggy-backed to these requests.
    void addErrorResponses(const KeeperRequestsForSessions & requests_for_sessions, Coordination::Error error, bool may_have_dependent_reads = true);

    /// Forcefully wait for result and sets errors if something when wrong.
    /// Clears both arguments
    nuraft::ptr<nuraft::buffer> forceWaitAndProcessResult(
        RaftAppendResult & result, KeeperRequestsForSessions & requests_for_sessions, bool clear_requests_on_success);

    using ConfigCheckCallback = std::function<bool(KeeperServer * server)>;
    void executeClusterUpdateActionAndWaitConfigChange(const ClusterUpdateAction & action, ConfigCheckCallback check_callback, size_t max_action_wait_time_ms, int64_t retry_count);

    /// Verify some logical issues in command, like duplicate ids, wrong leadership transfer and etc
    void checkReconfigCommandPreconditions(Poco::JSON::Object::Ptr reconfig_command);
    void checkReconfigCommandActions(Poco::JSON::Object::Ptr reconfig_command);

public:
    using SessionAndXID = std::pair</*session ID*/ int64_t, Coordination::XID>;

    struct SessionAndXIDHash
    {
        uint64_t operator()(std::pair<int64_t, Coordination::XID>) const;
    };

    std::mutex read_request_queue_mutex;

    /// Local read requests that are piggy-backed to other raft requests.
    /// Map: raft request -> read requests.
    /// The read must be executed immediately after the corresponding raft request is committed.
    /// Note that the read may belong to a different session than the raft request.
    /// (So e.g. we can't remove session ID from this map when the session is closed.)
    std::unordered_map<SessionAndXID, KeeperRequestsForSessions, SessionAndXIDHash> read_request_queue;

    /// Just allocate some objects, real initialization is done by `initialize`
    KeeperDispatcher();

    /// Call shutdown
    ~KeeperDispatcher();

    /// Initialization from config.
    /// standalone_keeper -- we are standalone keeper application (not inside clickhouse server)
    /// 'macros' are used to substitute macros in endpoint of disks
    void initialize(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper, bool start_async, const MultiVersion<Macros>::Version & macros);

    void startServer();

    bool checkInit() const
    {
        return server && server->checkInit();
    }

    /// Is server accepting requests, i.e. connected to the cluster
    /// and achieved quorum
    bool isServerActive() const;

    void updateConfiguration(const Poco::Util::AbstractConfiguration & config, const MultiVersion<Macros>::Version & macros);
    void pushClusterUpdates(ClusterUpdateActions && actions);
    bool reconfigEnabled() const;

    /// Process reconfiguration 4LW command: rcfg, it's another option to update cluster configuration
    Poco::JSON::Object::Ptr reconfigureClusterFromReconfigureCommand(Poco::JSON::Object::Ptr reconfig_command);

    /// Signal TCP handlers to close connections before the full shutdown.
    void signalShutdown() { shutting_down.store(true, std::memory_order_relaxed); }

    /// Returns true if signalShutdown() was called.
    bool isShuttingDown() const { return shutting_down.load(std::memory_order_relaxed); }

    /// Shutdown internal keeper parts (server, state machine, log storage, etc)
    void shutdown();

    void forceRecovery();

    /// Put request to ClickHouse Keeper
    bool putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, bool use_xid_64);

    /// Get new session ID
    int64_t getSessionID(int64_t session_timeout_ms);

    /// Register session and subscribe for responses with callback.
    /// The callback must be safe for concurrent invocation — see ZooKeeperResponseCallback.
    void registerSession(int64_t session_id, ZooKeeperResponseCallback callback);

    /// Call if we don't need any responses for this session no more (session was expired)
    void finishSession(int64_t session_id);

    /// Invoked when a request completes.
    void updateKeeperStatLatency(uint64_t process_time_ms, uint64_t subrequests = 1);

    /// Are we leader
    bool isLeader() const
    {
        return server->isLeader();
    }

    bool isFollower() const
    {
        return server->isFollower();
    }

    const char * getRoleString() const
    {
        if (isLeader())
            return "leader";
        if (isFollower())
            return "follower";
        if (isObserver())
            return "observer";
        return "unknown";
    }

    bool hasLeader() const
    {
        return server->isLeaderAlive();
    }

    bool isObserver() const
    {
        return server->isObserver();
    }

    bool isExceedingMemorySoftLimit() const
    {
        return server->isExceedingMemorySoftLimit();
    }

    uint64_t getLogDirSize() const;

    uint64_t getSnapDirSize() const;

    /// Request statistics such as qps, latency etc.
    KeeperConnectionStats & getKeeperConnectionStats()
    {
        return keeper_stats;
    }

    Keeper4LWInfo getKeeper4LWInfo() const;

    const IKeeperStateMachine & getStateMachine() const
    {
        return *server->getKeeperStateMachine();
    }

    const KeeperConfigurationPtr & getKeeperConfiguration() const
    {
        return server_config;
    }

    const KeeperContextPtr & getKeeperContext() const
    {
        return keeper_context;
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

    /// Create snapshot manually, return the last committed log index in the snapshot
    uint64_t createSnapshot()
    {
        return server->createSnapshot();
    }

    /// Get Raft information
    KeeperLogInfo getKeeperLogInfo()
    {
        return server->getKeeperLogInfo();
    }

    /// Request to be leader.
    bool requestLeader()
    {
        return server->requestLeader();
    }

    /// Yield leadership and become follower.
    void yieldLeadership()
    {
        server->yieldLeadership();
    }

    void recalculateStorageStats()
    {
        server->recalculateStorageStats();
    }

    static void cleanResources();

    std::optional<AuthenticationData> getAuthenticationData() const { return server->getAuthenticationData(); }
};

}

#endif
