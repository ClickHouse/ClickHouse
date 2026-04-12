#pragma once

#include <Interpreters/OpenTelemetrySpanLog.h>
#include "config.h"

#if USE_NURAFT

#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <functional>
#include <span>
#include <Coordination/KeeperServer.h>
#include <Coordination/Keeper4LWInfo.h>
#include <Coordination/KeeperConnectionStats.h>
#include <Coordination/KeeperRequestsQueue.h>
#include <Coordination/KeeperSessionRegistry.h>
#include <Coordination/KeeperSnapshotManagerS3.h>
#include <Common/MultiVersion.h>
#include <Common/Macros.h>
#include <Poco/JSON/Object.h>

namespace DB
{
/// Highlevel wrapper for ClickHouse Keeper.
/// Process user requests via consensus and return responses.
class KeeperDispatcher
{
private:
    using ClusterUpdateQueue = ConcurrentBoundedQueue<ClusterUpdateAction>;

    std::unique_ptr<KeeperRequestsQueue> requests_queue;
    KeeperSubqueuePtr system_subqueue;  /// For session-less requests (SessionID, dead-session Close).
    SnapshotsQueue snapshots_queue{1};

    /// More than 1k updates is definitely misconfiguration.
    ClusterUpdateQueue cluster_update_queue{1000};

    std::unique_ptr<KeeperSessionRegistry> session_registry;

    /// Reading and batching new requests from client handlers
    ThreadFromGlobalPool request_thread;
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

    KeeperSnapshotManagerS3 snapshot_s3;

    KeeperContextPtr keeper_context;

    /// Flag to signal TCP handlers that they should close connections.
    /// Set before the full shutdown() to allow handlers to exit promptly.
    std::atomic<bool> shutting_down{false};

    /// Thread put requests to raft
    void requestThread();
    /// Thread clean disconnected sessions from memory
    void sessionCleanerTask();
    /// Thread create snapshots in the background
    void snapshotThread();

    // TODO (myrrc) this should be removed once "reconfig" is stabilized
    void clusterUpdateWithReconfigDisabledThread();
    void clusterUpdateThread();

    /// Returns true if response was successfully sent to client, false if session doesn't exist on this node.
    bool routeResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr parsed_request = nullptr);

    /// Deliver error responses to sessions.
    void addErrorResponses(const KeeperRequestsForSessions & requests_for_sessions, Coordination::Error error);

    /// Send error responses AND notify sessions about failed writes
    /// so they can release stuck deferred reads.
    void failBatchSessions(const KeeperRequestsForSessions & batch, Coordination::Error error);

    /// Forcefully wait for result and sets errors if something went wrong.
    /// Clears both arguments
    nuraft::ptr<nuraft::buffer> forceWaitAndProcessResult(
        RaftAppendResult & result, KeeperRequestsForSessions & requests_for_sessions, bool clear_requests_on_success);

    using ConfigCheckCallback = std::function<bool(KeeperServer * server)>;
    void executeClusterUpdateActionAndWaitConfigChange(const ClusterUpdateAction & action, ConfigCheckCallback check_callback, size_t max_action_wait_time_ms, int64_t retry_count);

    /// Verify some logical issues in command, like duplicate ids, wrong leadership transfer and etc
    void checkReconfigCommandPreconditions(Poco::JSON::Object::Ptr reconfig_command);
    void checkReconfigCommandActions(Poco::JSON::Object::Ptr reconfig_command);

public:
    /// Just allocate some objects, real initialization is done by `initialize` method
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

    /// Put request to ClickHouse Keeper. Returns ZOK on success or
    /// a specific error code on rejection (queue full, memory limit, etc.).
    /// The overload with `session` avoids a `findSession` lookup per request.
    Coordination::Error putRequest(SessionRequestPtr keeper_req);
    Coordination::Error putRequest(SessionRequestPtr keeper_req, const KeeperSessionPtr & session);

    /// Put local read request to ClickHouse Keeper (bypasses session FIFO).
    /// Used by KeeperOverDispatcher for in-process reads.
    bool putLocalReadRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id);


    /// Get new session ID
    int64_t getSessionID(int64_t session_timeout_ms);

    /// Register a session with a response callback.
    /// All response delivery goes through the callback (both TCP handler and
    /// KeeperOverDispatcher paths).
    KeeperSessionPtr registerSession(int64_t session_id, KeeperSession::ResponseCallback callback);

    /// Terminate a session: deliver one error response (if callback is alive),
    /// then detach from registry. Idempotent — second call for same session is a no-op.
    void terminateSession(int64_t session_id, Coordination::Error error = Coordination::Error::ZSESSIONEXPIRED);

    /// Dispatch a batch of local reads to KeeperServer.
    void dispatchLocalReads(std::span<SessionRequestPtr> batch);

    /// Called by KeeperServer after each Raft commit.
    void onRaftCommit(uint64_t log_idx, const KeeperRequestForSession & request);

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
