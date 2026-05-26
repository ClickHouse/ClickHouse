#pragma once

#include "config.h"

#if USE_NURAFT

#include <Interpreters/OpenTelemetrySpanLog.h>
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
#include <Coordination/KeeperRequestDispatcherOld.h>
#include <Coordination/KeeperRequestDispatcher.h>

namespace DB
{

/// KeeperRequestDispatcherOld dispatches regular request processing, this class manages everything else:
/// snapshots, new session id assignment, expired session cleanup etc.
class KeeperDispatcher
{
private:
    using ClusterUpdateQueue = ConcurrentBoundedQueue<ClusterUpdateAction>;

    SnapshotsQueue snapshots_queue{1};

    /// More than 1k updates is definitely misconfiguration.
    ClusterUpdateQueue cluster_update_queue{1000};

    /// When client connects to the server for the first time it doesn't have session_id.
    /// It request it from server. We give temporary internal id for such requests just to match
    /// client with its response. This is a map of in-progress SessionID requests.
    /// After a proper session id is assigned, session lives in KeeperRequestDispatcherOld's session map.
    mutable std::mutex new_session_id_mutex;
    std::unordered_map<int64_t, std::promise<int64_t>> new_session_id_requests;

    /// Cleaning old dead sessions
    ThreadFromGlobalPool session_cleaner_thread;
    /// TTL expiry: leader enqueues TryRemove for expired empty nodes
    ThreadFromGlobalPool ttl_garbage_collector_thread;
    /// Dumping new snapshots to disk
    ThreadFromGlobalPool snapshot_thread;
    /// Apply or wait for configuration changes
    ThreadFromGlobalPool update_configuration_thread;

    /// RAFT wrapper.
    std::unique_ptr<KeeperServer> server;

    /// Exactly one of these is non-null.
    /// Hopefully KeeperRequestDispatcher will work well and we'll delete KeeperRequestDispatcherOld soon.
    std::unique_ptr<KeeperRequestDispatcherOld> dispatcher_old;
    std::unique_ptr<KeeperRequestDispatcher> dispatcher;

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

    /// Thread clean disconnected sessions from memory
    void sessionCleanerTask();
    /// Thread create snapshots in the background
    void snapshotThread();

    // TODO (myrrc) this should be removed once "reconfig" is stabilized
    void clusterUpdateWithReconfigDisabledThread();
    void clusterUpdateThread();

    using ConfigCheckCallback = std::function<bool(KeeperServer * server)>;
    void executeClusterUpdateActionAndWaitConfigChange(const ClusterUpdateAction & action, ConfigCheckCallback check_callback, size_t max_action_wait_time_ms, int64_t retry_count);

    /// Verify some logical issues in command, like duplicate ids, wrong leadership transfer and etc
    void checkReconfigCommandPreconditions(Poco::JSON::Object::Ptr reconfig_command);
    void checkReconfigCommandActions(Poco::JSON::Object::Ptr reconfig_command);

    void garbageCollectorThread(size_t batch_size);

    void onSessionIDResponse(const Coordination::ZooKeeperResponsePtr & response) noexcept;

public:
    KeeperDispatcher();

    /// Call shutdown
    ~KeeperDispatcher();

    /// Initialization from config.
    /// standalone_keeper -- we are standalone keeper application (not inside clickhouse server)
    /// 'macros' are used to substitute macros in endpoint of disks
    void initialize(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper, bool start_async, const MultiVersion<Macros>::Version & macros);

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
    /// `closed_all_connections` should be false if there may be any remaining KeeperTCPHandler instances.
    void shutdown(bool closed_all_connections);

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

    void onResponseDeallocated(const Coordination::ZooKeeperResponse & response);
};

}

#endif
